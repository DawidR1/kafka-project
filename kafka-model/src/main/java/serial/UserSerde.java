package serial;

import model.User;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class UserSerde implements Serde<User> {

    @Override
    public Serializer<User> serializer() {
        return new JsonSerializer<>();
    }

    @Override
    public Deserializer<User> deserializer() {
        return new JsonUserDeserializer();
    }
}
