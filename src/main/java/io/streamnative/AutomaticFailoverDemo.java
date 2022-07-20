package io.streamnative;

import io.streamnative.data.DataGenerator;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.AutoClusterFailover;

import java.util.Collections;
import java.util.concurrent.TimeUnit;


public class AutomaticFailoverDemo extends FailoverDemoBase {

    public PulsarClient getClient() throws PulsarClientException {
        if (client == null) {
            ServiceUrlProvider failover = AutoClusterFailover.builder()
                    .primary(ACTIVE_BROKER_URL)
                    .secondary(Collections.singletonList(STANDBY_BROKER_URL))
                    .failoverDelay(20, TimeUnit.SECONDS)
                    .switchBackDelay(20, TimeUnit.SECONDS)
                    .checkInterval(1000, TimeUnit.MILLISECONDS)
                    .secondaryAuthentication(null)
                    .build();

            client = PulsarClient.builder()
                    .serviceUrlProvider(failover)
                    .build();
        }
        return client;
    }

    public static void main(String[] args) throws PulsarClientException {
        AutomaticFailoverDemo demo = new AutomaticFailoverDemo();
        DataGenerator generator = new DataGenerator(demo.getProducer(TOPIC_NAME));
        generator.run();
    }
}
