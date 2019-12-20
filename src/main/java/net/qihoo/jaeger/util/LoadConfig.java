package net.qihoo.jaeger.util;

import net.qihoo.jaeger.core.SenderConfiguration;
import net.qihoo.jaeger.core.spi.Sender;
import lombok.Getter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;

import java.net.InetAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class LoadConfig {
    private static final Log LOG = LogFactory.getLog(LoadConfig.class);
    /**
     * The queue that will get all HTrace spans that are to be sent.
     */
    private BlockingQueue<Span> queue;
    /**
     * HTraceSpan -> ThriftSpan Converter Object.
     */
    private HTraceToThriftConverter hTraceToThriftConverter;
    /**
     * Default SenderConfiguration information.
     */
    private String agentHostName;
    private int agentPort;
    private int flushInterval;
    private int numThreads;
    private int queueCapacity;
    private String endPoint;
    private int pollTimeOut;
    /**
     * Default JaegerAgent PORT and HOST.
     */
    private static final String VISION_AGENT_HOSTNAME = "vision.agent-hostname";
    private static final String VISION_AGENT_PORT = "vision.agent-port";
    private static final int DEFAULT_VISION_AGENT_PORT = 5775;
    /**
     * Default refresh interval time.
     */
    private static final String VISION_FLUSH_INTERVAL = "vision.flush-interval";
    private static final int DEFAULT_JAEGER_FLUSH_INTERVAL = 20000;
    /**
     * Default number of Consumer threads.
     */
    private static final String CONSUMER_THREAD_KEY = "vision.consumer-threads";
    private static final int DEFAULT_CONSUMER_THREAD = 1;
    /**
     * Default Queue Capacity
     */
    private static final String CONSUMER_QUEUE_CAPACITY = "vision.queue-capacity";
    private static final int DEFAULT_CONSUMER_QUEUE_CAPACITY = Integer.MAX_VALUE;

    private static final String VISION_ENDPOINT = "vision.endpoint";
    private static final String DEFAULT_VISION_ENDPOINT = "null";

    private static final String POLL_TIMEOUT = "vision.poll-timeout";
    private static final int DEFAULT_POLL_TIMEOUT = 100;

    public LoadConfig(HTraceConfiguration conf) {
        try {
            agentHostName = conf.get(VISION_AGENT_HOSTNAME, InetAddress.getLocalHost().getHostAddress());
            agentPort = conf.getInt(VISION_AGENT_PORT, DEFAULT_VISION_AGENT_PORT);
            flushInterval = conf.getInt(VISION_FLUSH_INTERVAL, DEFAULT_JAEGER_FLUSH_INTERVAL);
            numThreads = conf.getInt(CONSUMER_THREAD_KEY, DEFAULT_CONSUMER_THREAD);
            queueCapacity = conf.getInt(CONSUMER_QUEUE_CAPACITY, DEFAULT_CONSUMER_QUEUE_CAPACITY);
            endPoint = conf.get(VISION_ENDPOINT, DEFAULT_VISION_ENDPOINT);
            pollTimeOut = conf.getInt(POLL_TIMEOUT, DEFAULT_POLL_TIMEOUT);
            this.queue = new LinkedBlockingQueue<Span>(queueCapacity);
            this.hTraceToThriftConverter = new HTraceToThriftConverter();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Sender loadSender() {
        SenderConfiguration senderConfiguration = new SenderConfiguration();
        senderConfiguration.withAgentHost(agentHostName).withAgentPort(agentPort);
        if (!endPoint.equals("null")){
            senderConfiguration.withEndpoint(String.format("http://%s/api/traces", endPoint));
        }
        return senderConfiguration.getSender();
    }
}
