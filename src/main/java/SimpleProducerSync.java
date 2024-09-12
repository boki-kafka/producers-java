import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer configuration setting
        // null, "hello world"
        Properties props = initProducerProps();

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello world 2");

        // KafkaProducer 메시지 동기 전송
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            logger.info("""
                    ###### record metadata received #####
                    "topic": {},
                    "partition": {},
                    "offset": {},
                    "timestamp": {},
                    """,
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                recordMetadata.timestamp()
            );
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }


    }

    private static Properties initProducerProps() {
        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        // props.setProperty("bootstrap.servers", "10.211.55.53:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.53:9092");

        // props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

}
