package net.qihoo.htrace.impl;

import jaeger.core.Configuration;
import jaeger.core.exceptions.SenderException;
import jaeger.core.senders.SenderResolver;
import jaeger.core.spi.Sender;
import lombok.ToString;
import net.qihoo.htrace.transition.HTraceToJaegerConverter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class JaegerSpanReceiver extends SpanReceiver {
    private static final Log LOG = LogFactory.getLog(JaegerSpanReceiver.class);

    /**
     * The queue that will get all HTrace spans that are to be sent.
     */
    private final BlockingQueue<Span> queue;

    /**
     * Boolean used to signal that the threads should end.
     */
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * The transport that the spans
     */
    private Sender sender;

    /***
     * Timed processing flush
     */
    private static Timer flushTimer;

    /***
     * object transition
     */
    private HTraceToJaegerConverter converter;

    /***
     * Consumer thread
     */
    private ExecutorService service;

    /***
     * HTrace Configuration
     */
    private HTraceConfiguration conf;

    /**
     * Default configuration information.
     */
    private static String agentHostName;
    private static int agentPort;
    private static int flushInterval;
    private static String samplerType;
    private static int samplerParam;
    private static int numThreads;

    private static final String JAEGER_AGENT_HOSTNAME = "jaeger.agent-hostname";

    private static final String JAEGER_AGENT_PORT = "jaeger.agent-port";
    private static final int DEFAULT_JAEGER_AGENT_PORT = -1;

    private static final String JAEGER_FLUSH_INTERVAL = "jaeger.flush-interval";
    private static final int DEFAULT_JAEGER_FLUSH_INTERVAL = 100;

    private static final String JAEGER_SAMPLER_TYPE = "jaeger.sampler-type";
    private static final String DEFAULT_JAEGER_SAMPLER_TYPE = "const";

    private static final String JAEGER_SAMPLER_PARAM = "jaeger.sampler-Param";
    private static final int DEFAULT_JAEGER_SAMPLER_PARAM = 1;

    /**
     * Default number of threads to use.
     */
    private static final int DEFAULT_NUM_THREAD = 1;
    private static final String NUM_THREAD_KEY = "jaeger.num-threads";

    /**
     * How long this receiver will try and wait for all threads to shutdown.
     */
    private static final int SHUTDOWN_TIMEOUT = 30;

    /**
     * The thread factory used to create new ExecutorService.
     * <p/>
     * This will be the same factory for the lifetime of this object so that
     * no thread names will ever be duplicated.
     */
    private final ThreadFactory tf = new ThreadFactory() {
        private final AtomicLong receiverIdx = new AtomicLong(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName(String.format("JaegerSpanReceiver-%d", receiverIdx.getAndIncrement()));
            return t;
        }
    };

    public JaegerSpanReceiver(HTraceConfiguration conf) {
        this.conf = conf;
        loadConfig();
        this.queue = new ArrayBlockingQueue<Span>(1000);
        this.converter = new HTraceToJaegerConverter();
//        this.sender = SenderResolver.resolve();
        this.sender = new Configuration.ReporterConfiguration().getSenderConfiguration().getSender();
        StartThread();
    }

    protected void loadConfig() {
        try {
            agentHostName = conf.get(JAEGER_AGENT_HOSTNAME, InetAddress.getLocalHost().getHostAddress());
            agentPort = conf.getInt(JAEGER_AGENT_PORT, DEFAULT_JAEGER_AGENT_PORT);
            flushInterval = conf.getInt(JAEGER_FLUSH_INTERVAL, DEFAULT_JAEGER_FLUSH_INTERVAL);
            samplerType = conf.get(JAEGER_SAMPLER_TYPE, DEFAULT_JAEGER_SAMPLER_TYPE);
            samplerParam = conf.getInt(JAEGER_SAMPLER_PARAM, DEFAULT_JAEGER_SAMPLER_PARAM);
            numThreads = conf.getInt(NUM_THREAD_KEY, DEFAULT_NUM_THREAD);

            Configuration config = new Configuration();
            Configuration.SenderConfiguration sender = new Configuration.SenderConfiguration();
            sender.withAgentHost(agentHostName).withAgentPort(agentPort);
            config.withReporter(new Configuration.ReporterConfiguration().withSender(sender).withFlushInterval(flushInterval).withLogSpans(false));
            config.withSampler(new Configuration.SamplerConfiguration().withType(samplerType).withParam(samplerParam));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void StartThread() {
        // If there are already threads runnnig tear them down.
        if (this.service != null) {
            this.service.shutdownNow();
            this.service = null;
        }
        this.service = Executors.newFixedThreadPool(numThreads, tf);
        this.service.submit(new WriteSpanRunnable());

        flushTimer = new Timer("jaeger.RemoteReporter-FlushTimer", true /* isDaemon */);
        flushTimer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            sender.flush();
                        } catch (SenderException e) {
                            e.printStackTrace();
                        }
                    }
                },
                flushInterval,
                flushInterval);
    }

    private class WriteSpanRunnable implements Runnable {
        /**
         * This runnable converts an HTrace span to a Thrift span and sends it across the transport
         * <p/>
         * Here is a little ascii art which shows the above transformation:
         * <pre>
         *  +------------+   +------------+              +-----------------+
         *  | HTrace Span|-->|Thrift Span | ===========> | Jaeger Agent|
         *  +------------+   +------------+ (transport)  +-----------------+
         *  </pre>
         */
        @Override
        public void run() {
            while (running.get() || queue.size() > 0) {
                //TODO (clenene) the following code (try / catch) is duplicated in / from FlumeSpanReceiver
                try {
                    Span htraceSpan = queue.take();
                    jaeger.thrift.thriftjava.Span thriftSpan = converter.convert(htraceSpan);
                    sender.append(thriftSpan);
                } catch (InterruptedException e) {
                    LOG.error(e);
                    throw new RuntimeException(e);
                } catch (SenderException e) {
                    e.printStackTrace();
                }
            }
            try {
                sender.flush();
                closeClient();
            } catch (SenderException e) {
                e.printStackTrace();
            }
        }

        /**
         * Close out the connection.
         */
        private void closeClient() {
            try {
                sender.close();
            } catch (SenderException e) {
                System.err.println();
            }
        }
    }

    @Override
    public void receiveSpan(Span span) {
        if (running.get()) {
            try {
                this.queue.add(span);
            } catch (IllegalStateException e) {
                System.err.println("Error trying to append span (" + span.getDescription() + ") to the queue."
                        + "  Blocking Queue was full.");
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        service.shutdown();
        try {
            if (!service.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
                LOG.error("Was not able to process all remaining spans to write upon closing in: " +
                        SHUTDOWN_TIMEOUT + " " + TimeUnit.SECONDS + ". There could be un-sent spans still left." +
                        "  They have been dropped.");
            }
        } catch (InterruptedException e1) {
            LOG.warn("Thread interrupted when terminating executor.", e1);
        }
    }
}
