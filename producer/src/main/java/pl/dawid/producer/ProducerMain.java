package pl.dawid.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProducerMain {
    private static final String TOPIC = "user-general-topic";
    private static final Logger logger = LoggerFactory.getLogger(ProducerMain.class.getName());

    public static void main(String[] args) {
        var client = new UserGeneratorClient();
        var producer = ProducerFactory.getIdempotenceProducer();
        var executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(executeTask(client, producer), 0, 5, TimeUnit.SECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownHook(producer, executorService)));
    }

    private static Runnable executeTask(UserGeneratorClient client, KafkaProducer<Integer, String> kafkaProducer) {
        return () -> {
            var record = new ProducerRecord<Integer, String>(TOPIC, null, client.getRandomUser());
            kafkaProducer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    logger.error("Send failed for record: {}", record, e);
                } else {
                    logger.info("Record was send {}", record);
                }
            });
        };
    }

    private static void shutdownHook(KafkaProducer<Integer, String> producer, ScheduledExecutorService executorService) {
        logger.info("stopping application...");
        producer.close();
        executorService.shutdown();
    }
}
