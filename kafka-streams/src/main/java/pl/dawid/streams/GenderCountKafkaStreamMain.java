package pl.dawid.streams;

import model.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import serial.UserSerde;

import java.util.Properties;

public class GenderCountKafkaStreamMain {


    public static void main(String[] args) {
        var properties = PropertiesFactory.getDefaultProperties("gender-kafka-streams-app",Serdes.String(), new UserSerde());
        var streamsBuilder = new StreamsBuilder();
        KStream<String, User> stream = streamsBuilder.stream(Mappings.USER_TOPIC);
        KTable<String, Long> genderKTable = stream
                .selectKey((key, user) -> user.getGender())
                .groupByKey()
                .count();
        genderKTable.toStream().to(Mappings.GENDER_COUNT_TOPIC, Produced.with(Serdes.String(),Serdes.Long()));
        var kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }
}
