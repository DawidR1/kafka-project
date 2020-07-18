package pl.dawid.streams;

import model.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import serial.JsonSerializer;
import serial.JsonUserDeserializer;
import serial.UserSerde;

import java.util.Properties;

public class PropertiesFactory {

    private static Properties getDefault(String appId) {
        var properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return properties;
    }

    public static Properties getDefaultProperties(String appId, Serde<?> keySerde, Serde<?> valueSerde) {
        var properties = getDefault(appId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        return properties;
    }
}
