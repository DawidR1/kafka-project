package pl.dawid;

import io.vavr.collection.List;
import model.User;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serial.JsonUserDeserializer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static io.vavr.collection.Stream.ofAll;

public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
    private static final CountDownLatch latch = new CountDownLatch(3);

    public static void main(String[] args) {
        var executorService = Executors.newFixedThreadPool(3);
        var userConsumer = ConsumerFactory.<String, User>getDefaultConsumer("user-topic", StringDeserializer.class, JsonUserDeserializer.class);
        var countryAgeRangeConsumer = ConsumerFactory.<String, Long>getDefaultConsumer("country-age-range-topic", StringDeserializer.class, StringDeserializer.class);
        var genderConsumer = ConsumerFactory.<String, Long>getDefaultConsumer("gender-count-topic", StringDeserializer.class, LongDeserializer.class);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownHook(userConsumer, countryAgeRangeConsumer, genderConsumer)));
        executorService.execute(() -> retrieveRecordsFromTopic(countryAgeRangeConsumer));
        executorService.execute(() -> retrieveRecordsFromTopic(genderConsumer));
        executorService.execute(() -> retrieveRecordsFromTopic(userConsumer));
    }

    private static void retrieveRecordsFromTopic(KafkaConsumer<?, ?> consumer) {
        try (consumer) {
            while (true) {
                ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(100));
                ofAll(records)
                        .map(record -> String.format("Topic: %s Record: {key: %s; value: %s}", record.topic(), record.key().toString(), record.value().toString()))
                        .forEach(logger::info);
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            logger.info("closing consumer");
        } finally {
            latch.countDown();
        }
    }

    private static void shutdownHook(KafkaConsumer<?, ?>... consumers) {
        logger.info("stopping application...");
        List.of(consumers).forEach(KafkaConsumer::wakeup);
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Thread was interrupted", e);
        }
        logger.info("Application was closed");
    }
}
