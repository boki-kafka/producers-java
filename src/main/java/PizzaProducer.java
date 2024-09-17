import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class PizzaProducer {

    private static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());

    public static void sendPizzaMessage(
        KafkaProducer<String, String> kafkaProducer,
        String topicName,
        int iterCnt,
        int interIntervalMillis,
        int intervalMillis,
        int intervalCount,
        boolean isSync
    ) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        long seed = 2024;
        Random random = new Random(seed);
        Faker faker = new Faker(random);

        int iterSeq = 0;

        while (iterSeq++ != iterCnt) {
            HashMap<String, String> pMessage = pizzaMessage.produceMsg(faker, random, iterSeq);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName, pMessage.get("key"), pMessage.get("message")
            );
            sendMessage(kafkaProducer, record, pMessage, isSync);

            if ((intervalCount > 0 && (iterSeq % intervalCount) == 0)) {
                try {
                    logger.info("###### IntervalCount: {} intervalMillis: {} ######",
                        intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis: {}", interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    public static void sendMessage(
        KafkaProducer<String, String> producer,
        ProducerRecord<String, String> producerRecord,
        Map<String, String> pMessage, boolean isSync
    ) {
        if (!isSync) {
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("""
                            async message: {} / partition: {} / offset: {}
                            """,
                        pMessage.get("key"),
                        metadata.partition(),
                        metadata.offset()
                    );
                }
                else {
                    logger.error("exception error from broker: {}", exception.getMessage());
                }
            });
        }
        else {
            try {
                RecordMetadata metadata = producer.send(producerRecord).get();
                logger.info("""
                        async message: {} / partition: {} / offset: {}
                        """,
                    pMessage.get("key"),
                    metadata.partition(),
                    metadata.offset()
                );
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            } catch (ExecutionException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        String topicName = "pizza-topic";

        Properties props = initProducerProps(
            StringSerializer.class,
            StringSerializer.class
        );

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        sendPizzaMessage(
            producer, topicName,
            -1, 100, 1000, 100, false
        );

        producer.close();
    }

    private static <K, V> Properties initProducerProps(
        Class<? extends Serializer<K>> keySerClass,
        Class<? extends Serializer<V>> valueSerClass
    ) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "10.211.55.53:9092");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, keySerClass.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerClass.getName());
        return props;
    }

}
