package serial;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.User;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JsonUserDeserializer implements Deserializer<User> {


    public JsonUserDeserializer() {
    }

    @Override
    public User deserialize(String s, byte[] bytes) {
        return deserializeJson(bytes);
    }

    @Override
    public User deserialize(String topic, Headers headers, byte[] data) {
        return deserializeJson(data);
    }

    private User deserializeJson(byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return new ObjectMapper().readValue(data, User.class);
        } catch (IOException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }
}
