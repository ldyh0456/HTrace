package net.qihoo.jaeger.impl;

import net.qihoo.jaeger.core.exceptions.SenderException;
import net.qihoo.jaeger.core.spi.Sender;
import net.qihoo.jaeger.thrift.thriftjava.ThriftSpan;
import net.qihoo.jaeger.util.FlushSpan;
import net.qihoo.jaeger.util.HTraceToThriftConverter;
import net.qihoo.jaeger.util.LoadConfig;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.MilliSpan;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;

import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class JaegerSpanReceiver extends SpanReceiver {
    private static final Log LOG = LogFactory.getLog(JaegerSpanReceiver.class);
    /**
     * Consumer threads.
     */
    private ExecutorService consumerThread;
    /**
     * The queue that will get all HTrace spans that are to be sent.
     */
    private BlockingQueue<Span> queue;
    /**
     * HTraceSpan -> ThriftSpan Converter Object.
     */
    private HTraceToThriftConverter hTraceToThriftConverter;

    private ScheduledExecutorService flushTimer;
    /**
     * Default SenderConfiguration information.
     */
    private static int flushInterval;
    private static int numThreads;
    /**
     * Boolean used to signal that the threads should end.
     */
    private final AtomicBoolean running = new AtomicBoolean(true);
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
        LoadConfig configuration = new LoadConfig(conf);
        flushInterval = configuration.getFlushInterval();
        numThreads = configuration.getNumThreads();
        queue = configuration.getQueue();
        hTraceToThriftConverter = configuration.getHTraceToThriftConverter();
        StartThread(configuration);
    }

    private void StartThread(LoadConfig configuration) {
        // If there are already threads runnnig tear them down.
        if (this.consumerThread != null) {
            this.consumerThread.shutdownNow();
            this.consumerThread = null;
        }
        this.consumerThread = Executors.newFixedThreadPool(numThreads, tf);
        for (int i = 0; i < numThreads; i++) {
            this.consumerThread.submit(new QueueProcessor(configuration));
        }
        if (flushInterval != 0){
            this.flushTimer = Executors.newScheduledThreadPool(1);
            this.flushTimer.scheduleWithFixedDelay(
                    new TimerTask() {
                        @Override
                        public void run() {
                            queue.offer(new FlushSpan());
                        }
                    },
                    0,
                    flushInterval,
                    TimeUnit.MILLISECONDS);
        }
    }

    private class QueueProcessor implements Runnable {
        private Sender sender;
        private int pollTimeOut;

        public QueueProcessor(LoadConfig configuration) {
            sender = configuration.loadSender();
            pollTimeOut = configuration.getPollTimeOut();
        }

        /**
         * This runnable converts an HTrace span to a Thrift span and sends it across the transport
         * <p>
         * +------------+   +------------+              +-----------------+
         * | HTrace Span|-->|Thrift Span | ===========> | Jaeger Agent|
         * +------------+   +------------+ (tranSport)  +-----------------+
         */
        @Override
        public void run() {
            while (running.get() || queue.size() > 0) {
                //TODO (clenene) the following code (try / catch) is duplicated in / from FlumeSpanReceiver
                try {
                    Span htraceSpan = queue.poll(pollTimeOut, TimeUnit.MILLISECONDS);
                    if (htraceSpan != null){
                        if (htraceSpan instanceof FlushSpan) {
                            sender.flush();
                        } else if (htraceSpan instanceof MilliSpan) {
                            ThriftSpan thriftSpan = hTraceToThriftConverter.convert(htraceSpan);
                            sender.append(thriftSpan);
                        } else {
                            running.set(false);
                            LOG.error("Unknown type exception, Please confirm Span type. ");
                        }
                    }
                } catch (InterruptedException e) {
                    running.set(false);
                    LOG.error("Getting HTrace Span from the queue is InterruptedException." + ExceptionUtils.getStackTrace(e));
                } catch (SenderException e) {
                    LOG.error("Using Sender exception. " + ExceptionUtils.getStackTrace(e));
                }
            }
            try {
                sender.close();
            } catch (SenderException e) {
                LOG.error("Failed to close Sender. " + ExceptionUtils.getStackTrace(e));
            }
        }
    }

    @Override
    public void receiveSpan(Span span) {
        if (running.get()) {
            if (!this.queue.offer(span)) {
                LOG.warn("Error trying to append span (Description: " + span.getDescription()
                        + ". TracerId: " + span.getTracerId() + ". SpanId: " + span.getSpanId() + ") to the queue."
                        + "  Blocking Queue was full. QueueSize: " + queue.size());
            }
        } else {
            LOG.warn("The consumer thread has closed and stopped receiving HTrace. ");
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumerThread.shutdown();
        flushTimer.shutdown();
        try {
            if (!consumerThread.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
                LOG.error("Was not able to process all remaining spans to write upon closing in: " +
                        SHUTDOWN_TIMEOUT + " " + TimeUnit.SECONDS + ". There could be un-sent spans still left." +
                        "  They have been dropped.");
            }
        } catch (InterruptedException e1) {
            LOG.warn("Thread interrupted when terminating executor.", e1);
        }
    }
}
