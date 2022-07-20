package io.streamnative;

import org.apache.pulsar.client.api.*;

public abstract class FailoverDemoBase {

    protected static final String TOPIC_NAME = "persistent://public/default/failover";
    protected static final String ACTIVE_BROKER_URL = "pulsar://127.0.0.1:6650";
    protected static final String STANDBY_BROKER_URL = "pulsar://192.168.1.121:6650";

    protected PulsarClient client;
    protected Producer<String> producer;

    public abstract PulsarClient getClient() throws PulsarClientException;

    public Producer<String> getProducer(String topicName) throws PulsarClientException {
        if (producer == null) {
            producer = getClient().newProducer(Schema.STRING)
                    .topic(topicName)
                    .create();
        }
        return producer;
    }
}
