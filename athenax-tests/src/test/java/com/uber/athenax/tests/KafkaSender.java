package com.uber.athenax.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaSender {

    static final String DEST_TOPIC = "bar";
    static final String SOURCE_TOPIC = "foo1";

    private static final long STABILIZE_SLEEP_DELAYS = 3000;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static String brokerAddress = "node3:9092";

    private static KafkaProducer<byte[], byte[]> getProducer(String brokerList) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return new KafkaProducer<>(prop);
    }

    public static void main(String[] args) {
        int i = 0;
        while (true) {
            try (KafkaProducer<byte[], byte[]> producer = getProducer(brokerAddress)) {
                try {
                    producer.send(new ProducerRecord<>(SOURCE_TOPIC, MAPPER.writeValueAsBytes(Collections.singletonMap("id", i++))));
                    Thread.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
