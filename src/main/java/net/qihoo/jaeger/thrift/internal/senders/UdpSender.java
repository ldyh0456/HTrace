/*
 * Copyright (c) 2016, Uber Technologies, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package net.qihoo.jaeger.thrift.internal.senders;

import net.qihoo.jaeger.thrift.agent.Agent;
import net.qihoo.jaeger.core.exceptions.SenderException;
import net.qihoo.jaeger.thrift.internal.protocols.ThriftUdpTransport;
import net.qihoo.jaeger.thrift.thriftjava.Batch;
import net.qihoo.jaeger.thrift.thriftjava.ThriftSpan;
import lombok.ToString;
import net.qihoo.jaeger.thrift.thriftjava.Process;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

@ToString
public class UdpSender extends ThriftSender {
    private static final Log LOG = LogFactory.getLog(UdpSender.class);
    public static final String DEFAULT_AGENT_UDP_HOST = "localhost";
    public static final int DEFAULT_AGENT_UDP_COMPACT_PORT = 6831;

    @ToString.Exclude
    private Agent.Client agentClient;
    @ToString.Exclude
    private ThriftUdpTransport udpTransport;

    /**
     * This constructor expects Jaeger running running on {@value #DEFAULT_AGENT_UDP_HOST}
     * and port {@value #DEFAULT_AGENT_UDP_COMPACT_PORT}
     */
    public UdpSender() {
        this(DEFAULT_AGENT_UDP_HOST, DEFAULT_AGENT_UDP_COMPACT_PORT, 0);
    }

    /**
     * @param host          host e.g. {@value DEFAULT_AGENT_UDP_HOST}
     * @param port          port e.g. {@value DEFAULT_AGENT_UDP_COMPACT_PORT}
     * @param maxPacketSize if 0 it will use ThriftUdpTransport#MAX_PACKET_SIZE
     */
    public UdpSender(String host, int port, int maxPacketSize) {
        super(ProtocolType.Compact, maxPacketSize);

        if (host == null || host.length() == 0) {
            host = DEFAULT_AGENT_UDP_HOST;
        }

        if (port == 0) {
            port = DEFAULT_AGENT_UDP_COMPACT_PORT;
        }

        udpTransport = ThriftUdpTransport.newThriftUdpClient(host, port);
        agentClient = new Agent.Client(protocolFactory.getProtocol(udpTransport));
    }

    @Override
    public void send(Process process, List<ThriftSpan> spans) {
        try {
            agentClient.emitBatch(new Batch(process, spans));
        } catch (Throwable t) {
            LOG.error(new SenderException(String.format("Could not send %d spans", spans.size()), t, spans.size()));
        }
    }

    @Override
    public int close() throws SenderException {
        try {
            return super.close();
        } finally {
            udpTransport.close();
        }
    }
}
