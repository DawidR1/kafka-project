package pl.dawid.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.control.Try;
import model.Result;
import model.User;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serial.UserSerde;

import static org.apache.kafka.common.serialization.Serdes.String;

public class UserKafkaStreamMain {

    private static final Logger logger = LoggerFactory.getLogger(UserKafkaStreamMain.class.getName());

    public static void main(String[] args) {
        var properties = PropertiesFactory.getDefaultProperties("user-kafka-streams-app", String(), String());
        var streamBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamBuilder.stream(Mappings.USER_GENERAL_TOPIC);
        KStream<String, User> userStream = stream
                .mapValues(mapToUserResults())
                .filter((key, value) -> value != null)
                .flatMapValues(Result::getResults)
                .filter((key, user) -> user.getId().getValue() != null)
                .selectKey((key, user) -> user.getId().getValue());
        userStream.to(Mappings.USER_TOPIC, Produced.with(String(), new UserSerde()));
        var streams = new KafkaStreams(streamBuilder.build(), properties);
        streams.cleanUp();
        streams.start();
    }

    private static ValueMapper<String, Result> mapToUserResults() {
        return value ->
                Try.of(() -> new ObjectMapper().readValue(value, Result.class))
                        .onFailure(throwable -> logger.error("Error happened when processing json to object, Object: {}; this object will be skipped", value, throwable))
                        .getOrNull();
    }
}
