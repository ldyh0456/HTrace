package net.qihoo.htrace.impl;

import jaeger.core.Configuration;
import jaeger.core.exceptions.SenderException;
import jaeger.core.spi.Sender;
import net.qihoo.htrace.util.HTraceToThriftConverter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;

import java.net.InetAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class JaegerSpanReceiver extends SpanReceiver {
    private static final Log LOG = LogFactory.getLog(JaegerSpanReceiver.class);
    /**
     * The transport that the spans.
     */
    private Sender sender;
    /**
     * Consumer threads consume queues.
     */
    private ExecutorService service;
    /**
     * HTrace Configuration.
     */
    private HTraceConfiguration conf;
    /**
     * The queue that will get all HTrace spans that are to be sent.
     */
    private final BlockingQueue<Span> queue;
    /**
     * HTraceSpan -> ThriftSpan Converter Object.
     */
    private HTraceToThriftConverter converter;
    /**
     * Boolean used to signal that the threads should end.
     */
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Configuration.ReporterConfiguration reporterConfig = new Configuration.ReporterConfiguration();

    /**
     * Default configuration information.
     */
    private static String agentHostName;
    private static int agentPort;
    private static int flushInterval;
    private static int numThreads;

    /**
     * Default JaegerAgent PORT and HOST
     */
    private static final String JAEGER_AGENT_HOSTNAME = "jaeger.agent-hostname";
    private static final String JAEGER_AGENT_PORT = "jaeger.agent-port";
    private static final int DEFAULT_JAEGER_AGENT_PORT = -1;
    /**
     * Default refresh interval time
     */
    private static final String JAEGER_FLUSH_INTERVAL = "jaeger.flush-interval";
    private static final int DEFAULT_JAEGER_FLUSH_INTERVAL = 20000;
    /**
     * Default number of Consumer threads .
     */
    private static final String NUM_THREAD_KEY = "jaeger.num-threads";
    private static final int DEFAULT_NUM_THREAD = 1;
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
        this.converter = new HTraceToThriftConverter();
        this.sender = reporterConfig.getSenderConfiguration().getSender();
        StartThread();
    }

    private void loadConfig() {
        try {
            agentHostName = conf.get(JAEGER_AGENT_HOSTNAME, InetAddress.getLocalHost().getHostAddress());
            agentPort = conf.getInt(JAEGER_AGENT_PORT, DEFAULT_JAEGER_AGENT_PORT);
            flushInterval = conf.getInt(JAEGER_FLUSH_INTERVAL, DEFAULT_JAEGER_FLUSH_INTERVAL);
            numThreads = conf.getInt(NUM_THREAD_KEY, DEFAULT_NUM_THREAD);
            Configuration.SenderConfiguration sender = new Configuration.SenderConfiguration();
            sender.withAgentHost(agentHostName).withAgentPort(agentPort);
            reporterConfig.withSender(sender).withFlushInterval(flushInterval).withLogSpans(false);
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

        Timer flushTimer = new Timer("jaeger.RemoteReporter-FlushTimer", true /* isDaemon */);
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
         *
         *  +------------+   +------------+              +-----------------+
         *  | HTrace Span|-->|Thrift Span | ===========> | Jaeger Agent|
         *  +------------+   +------------+ (transport)  +-----------------+
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
