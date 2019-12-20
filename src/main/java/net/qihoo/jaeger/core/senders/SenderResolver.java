/*
 * Copyright (c) 2018, The Jaeger Authors
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

package net.qihoo.jaeger.core.senders;

import net.qihoo.jaeger.core.spi.*;
import lombok.extern.slf4j.Slf4j;
import net.qihoo.jaeger.core.SenderConfiguration;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Provides a way to resolve an appropriate {@link Sender}
 */
@Slf4j
public class SenderResolver {

    /**
     * Resolves a {@link Sender} by passing {@link SenderConfiguration#fromEnv()} down to the
     * {@link SenderFactory}
     *
     * @return the resolved Sender, or NoopSender
     * @see #resolve(SenderConfiguration)
     */
    public static Sender resolve() {
        return resolve(SenderConfiguration.fromEnv());
    }

    /**
     * Resolves a sender by passing the given {@link SenderConfiguration} down to the
     * {@link SenderFactory}. The factory is loaded either based on the value from the environment variable
     * {@link SenderConfiguration#JAEGER_SENDER_FACTORY} or, in its absence or failure to deliver a {@link Sender},
     * via the {@link ServiceLoader}. If no factories are found, a {@link NoopSender} is returned. If multiple factories
     * are available, the factory whose {@link SenderFactory#getType()} matches the JAEGER_SENDER_FACTORY env var is
     * selected. If none matches, {@link NoopSender} is returned.
     *
     * @param SenderConfiguration the SenderConfiguration to pass down to the factory
     * @return the resolved Sender, or NoopSender
     */
    public static Sender resolve(SenderConfiguration SenderConfiguration) {
        Sender sender = null;
        ServiceLoader<SenderFactory> senderFactoryServiceLoader = ServiceLoader.load(SenderFactory.class,
                SenderFactory.class.getClassLoader());
        Iterator<SenderFactory> senderFactoryIterator = senderFactoryServiceLoader.iterator();

        boolean hasMultipleFactories = false;
        while (senderFactoryIterator.hasNext()) {
            SenderFactory senderFactory = senderFactoryIterator.next();

            if (senderFactoryIterator.hasNext()) {
                log.debug("There are multiple factories available via the service loader.");
                hasMultipleFactories = true;
            }

            if (hasMultipleFactories) {
                // we compare the factory name with JAEGER_SENDER_FACTORY, as a way to know which
                // factory the user wants:
                String requestedFactory = System.getProperty(SenderConfiguration.JAEGER_SENDER_FACTORY);
                if (senderFactory.getType().equals(requestedFactory)) {
                    log.debug(
                            String.format("Found the requested (%s) sender factory: %s",
                                    requestedFactory,
                                    senderFactory)
                    );

                    sender = getSenderFromFactory(senderFactory, SenderConfiguration);
                }
            } else {
                sender = getSenderFromFactory(senderFactory, SenderConfiguration);
            }
        }

        if (null != sender) {
            log.debug(String.format("Using sender %s", sender));
            return sender;
        } else {
            log.warn("No suitable sender found. Using NoopSender, meaning that data will not be sent anywhere!");
            return new NoopSender();
        }
    }

    private static Sender getSenderFromFactory(SenderFactory senderFactory, SenderConfiguration SenderConfiguration) {
        try {
            return senderFactory.getSender(SenderConfiguration);
        } catch (Exception e) {
            log.warn("Failed to get a sender from the sender factory.", e);
            return null;
        }
    }
}
