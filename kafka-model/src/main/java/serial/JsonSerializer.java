package serial;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(String s, T t) {
        return serializeData(t);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return serializeData(data);
    }

    private byte[] serializeData(T data) {
        if (data == null) {
            return null;
        }
        try {
            return new ObjectMapper().writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

}
