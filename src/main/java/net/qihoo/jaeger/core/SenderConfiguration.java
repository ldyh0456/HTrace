package net.qihoo.jaeger.core;

import net.qihoo.jaeger.core.senders.SenderResolver;
import net.qihoo.jaeger.core.spi.Sender;
import net.qihoo.jaeger.core.spi.SenderFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class SenderConfiguration {
    /**
     * Prefix for all properties used to configure the Jaeger tracer.
     */
    public static final String JAEGER_PREFIX = "JAEGER_";

    /**
     * The full URL to the {@code traces} endpoint, like https://jaeger-collector:14268/api/traces
     */
    public static final String JAEGER_ENDPOINT = JAEGER_PREFIX + "ENDPOINT";

    /**
     * The Auth Token to be added as "Bearer" on Authorization headers for requests sent to the endpoint
     */
    public static final String JAEGER_AUTH_TOKEN = JAEGER_PREFIX + "AUTH_TOKEN";

    /**
     * The Basic Auth username to be added on Authorization headers for requests sent to the endpoint
     */
    public static final String JAEGER_USER = JAEGER_PREFIX + "USER";

    /**
     * The Basic Auth password to be added on Authorization headers for requests sent to the endpoint
     */
    public static final String JAEGER_PASSWORD = JAEGER_PREFIX + "PASSWORD";

    /**
     * The host name used to locate the agent.
     */
    public static final String JAEGER_AGENT_HOST = JAEGER_PREFIX + "AGENT_HOST";

    /**
     * The port used to locate the agent.
     */
    public static final String JAEGER_AGENT_PORT = JAEGER_PREFIX + "AGENT_PORT";
    /**
     * The tracer level tags.
     */
    public static final String JAEGER_TAGS = JAEGER_PREFIX + "TAGS";
    /**
     * When there are multiple service providers for the {@link SenderFactory} available,
     * this var is used to select a {@link SenderFactory} by matching it with
     * {@link SenderFactory#getType()}.
     */
    public static final String JAEGER_SENDER_FACTORY = JAEGER_PREFIX + "SENDER_FACTORY";

    /**
     * A custom sender set by our consumers. If set, nothing else has effect. Optional.
     */
    private Sender sender;

    /**
     * The Agent Host. Has no effect if the sender is set. Optional.
     */
    private String agentHost;

    /**
     * The Agent Port. Has no effect if the sender is set. Optional.
     */
    private Integer agentPort;

    /**
     * The endpoint, like https://jaeger-collector:14268/api/traces
     */
    private String endpoint;

    /**
     * The Auth Token to be added as "Bearer" on Authorization headers for requests sent to the endpoint
     */
    private String authToken;

    /**
     * The Basic Auth username to be added on Authorization headers for requests sent to the endpoint
     */
    private String authUsername;

    /**
     * The Basic Auth password to be added on Authorization headers for requests sent to the endpoint
     */
    private String authPassword;

    public SenderConfiguration() {
    }

    public SenderConfiguration withAgentHost(String agentHost) {
        this.agentHost = agentHost;
        return this;
    }

    public SenderConfiguration withAgentPort(Integer agentPort) {
        this.agentPort = agentPort;
        return this;
    }

    public SenderConfiguration withEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public SenderConfiguration withAuthToken(String authToken) {
        this.authToken = authToken;
        return this;
    }

    public SenderConfiguration withAuthUsername(String username) {
        this.authUsername = username;
        return this;
    }

    public SenderConfiguration withAuthPassword(String password) {
        this.authPassword = password;
        return this;
    }

    /**
     * Returns a sender if one was given when creating the SenderConfiguration, or attempts to create a sender based on the
     * SenderConfiguration's state.
     *
     * @return the sender passed via the constructor or a properly configured sender
     */
    public Sender getSender() {
        if (sender == null) {
            sender = SenderResolver.resolve(this);
        }
        return sender;
    }

    /**
     * Attempts to create a new {@link SenderConfiguration} based on the environment variables
     *
     * @return a new sender SenderConfiguration based on environment variables
     */
    public static SenderConfiguration fromEnv() {
        String agentHost = getProperty(JAEGER_AGENT_HOST);
        Integer agentPort = getPropertyAsInt(JAEGER_AGENT_PORT);

        String collectorEndpoint = getProperty(JAEGER_ENDPOINT);
        String authToken = getProperty(JAEGER_AUTH_TOKEN);
        String authUsername = getProperty(JAEGER_USER);
        String authPassword = getProperty(JAEGER_PASSWORD);

        return new SenderConfiguration()
                .withAgentHost(agentHost)
                .withAgentPort(agentPort)
                .withEndpoint(collectorEndpoint)
                .withAuthToken(authToken)
                .withAuthUsername(authUsername)
                .withAuthPassword(authPassword);
    }

    private static String getProperty(String name) {
        return System.getProperty(name, System.getenv(name));
    }

    private static Integer getPropertyAsInt(String name) {
        String value = getProperty(name);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                log.error("Failed to parse integer for property '" + name + "' with value '" + value + "'", e);
            }
        }
        return null;
    }
}
