package io.streamnative;

import io.streamnative.data.DataGenerator;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.ControlledClusterFailover;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ControlledFailoverDemo extends FailoverDemoBase {

    private ServiceUrlProvider provider;

    private ServiceUrlProvider getSerivceProvider() throws IOException {
        if (provider == null) {
            Map<String, String> header = new HashMap<>();
            header.put("user", "my-name");
            header.put("pass", "mypass");

            provider = ControlledClusterFailover.builder()
                    .defaultServiceUrl(ACTIVE_BROKER_URL)
                    .checkInterval(1, TimeUnit.MINUTES)
                    .urlProvider("http://localhost:8080/test")
                    .urlProviderHeader(header)
                    .build();
        }

        return provider;
    }

    @Override
    public PulsarClient getClient() throws PulsarClientException {
        if (client == null) {
            try {
                PulsarClient client = PulsarClient.builder()
                        .serviceUrlProvider(getSerivceProvider())
                        .build();
            } catch (IOException e) {
               throw new PulsarClientException(e.getMessage());
            }
        }

        return client;
    }

    public static void main(String[] args) throws PulsarClientException {
        ControlledFailoverDemo demo = new ControlledFailoverDemo();
        DataGenerator generator = new DataGenerator(demo.getProducer(TOPIC_NAME));
        generator.run();
    }

}
