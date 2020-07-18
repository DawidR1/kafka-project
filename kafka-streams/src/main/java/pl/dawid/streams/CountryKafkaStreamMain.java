package pl.dawid.streams;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import model.User;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import serial.UserSerde;

import java.util.function.Predicate;

import static org.apache.kafka.common.serialization.Serdes.Integer;
import static org.apache.kafka.common.serialization.Serdes.String;
import static pl.dawid.streams.Mappings.COUNTRY_AVERAGE_AGE_TOPIC;

public class CountryKafkaStreamMain {
    private static final String COUNTRY_LIST_SEPARATOR = ", ";
    private static final Map<Predicate<Integer>, String> scopeByRange = HashMap.of(
            isInRange(0, 40), "Scope: < 30",
            isInRange(30, 40), "Scope: 30 - 40",
            isInRange(40, 50), "Scope: 40 - 50",
            isInRange(50, 60), "Scope: 50 - 60",
            isInRange(60, Integer.MAX_VALUE), "Scope: > 60");

    private static Predicate<Integer> isInRange(int lower, int upper) {
        return value -> value < upper && value >= lower;
    }

    private static Aggregator<String, String, String> countrySubtractor = (key, value, aggregateValue)
            -> List.of(aggregateValue.split(COUNTRY_LIST_SEPARATOR))
            .remove(value)
            .mkString(COUNTRY_LIST_SEPARATOR);

    private static KeyValueMapper<String, Integer, KeyValue<String, String>> ageRangeMapper = (key, value) -> {
        var newKey = scopeByRange.filterKeys(integerPredicate -> integerPredicate.test(value))
                .get()._2();
        return KeyValue.pair(newKey, key);
    };

    private static Aggregator<String, Integer, Integer> averageAgeAggregator = (key, value, aggregate) -> (aggregate + value) / 2;

    public static void main(String[] args) {
        var properties = PropertiesFactory.getDefaultProperties("country-kafka-streams-app", Serdes.String(), new UserSerde());
        var streamBuilder = new StreamsBuilder();
        KStream<String, User> stream = streamBuilder.stream(Mappings.USER_TOPIC);
        KTable<String, Integer> averageAgeByCountryKTable = stream
                .selectKey((key, value) -> value.getLocation().getCountry())
                .mapValues(value -> value.getDob().getAge())
                .groupByKey(Grouped.with(String(), Integer()))
                .aggregate(() -> 0
                        , averageAgeAggregator
                        , Materialized.with(String(), Integer())
                );
        averageAgeByCountryKTable.toStream().to(COUNTRY_AVERAGE_AGE_TOPIC, Produced.with(String(), Integer()));

        KTable<String, Integer> table = streamBuilder.table(COUNTRY_AVERAGE_AGE_TOPIC, Consumed.with(String(), Integer()));
        KTable<String, String> ageRangeByCountryKTable = table
                .groupBy(ageRangeMapper, Grouped.with(String(), String()))
                .aggregate(() -> "",
                        (key, value, aggregateValue) -> aggregateValue + COUNTRY_LIST_SEPARATOR + value,
                        countrySubtractor,
                        Materialized.with(String(), String()));
        ageRangeByCountryKTable.toStream().to(Mappings.COUNTRY_AGE_RANGE_TOPIC, Produced.with(String(), String()));
        var streams = new KafkaStreams(streamBuilder.build(), properties);
        streams.cleanUp();
        streams.start();
    }
}
