package pl.dawid;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerFactory {

    public static final String PEOPLE_TOPIC = "consumer-user-topic";

    public static <T, G> KafkaConsumer<T, G> getDefaultConsumer(String topic, Class<?> keyDeserializer, Class<?> valueDeserializer) {
        var properties = getDefaultProperties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        var consumer = new KafkaConsumer<T, G>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private static Properties getDefaultProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, PEOPLE_TOPIC);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return properties;
    }
}
