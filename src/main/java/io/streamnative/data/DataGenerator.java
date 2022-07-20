package io.streamnative.data;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

public class DataGenerator implements Runnable {

    private final Producer<String> producer;

    public DataGenerator(Producer<String> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        int counter = 0;
        while (true) {
            try {
                producer.newMessage().value("Message-" + counter++).send();
                Thread.sleep(750);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
