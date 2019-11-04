package jaeger.thrift.internal.senders;

import jaeger.core.Configuration ;
import jaeger.core.spi.Sender ;
import jaeger.core.spi.SenderFactory ;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
public class ThriftSenderFactory implements SenderFactory {
  @Override
  public Sender getSender(Configuration.SenderConfiguration conf) {
    log.debug("Using the UDP Sender to send spans to the agent.");
    return new UdpSender(
        stringOrDefault(conf.getAgentHost(), UdpSender.DEFAULT_AGENT_UDP_HOST),
        numberOrDefault(conf.getAgentPort(), UdpSender.DEFAULT_AGENT_UDP_COMPACT_PORT).intValue(),
        0 /* max packet size */);
  }

  @Override
  public String getType() {
    return "thrift";
  }

  private static String stringOrDefault(String value, String defaultValue) {
    return value != null && value.length() > 0 ? value : defaultValue;
  }

  private static Number numberOrDefault(Number value, Number defaultValue) {
    return value != null ? value : defaultValue;
  }
}
