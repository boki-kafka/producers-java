import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerASync {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer configuration setting
        // null, "hello world"
        Properties props = initProducerProps();

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello world 2");

        /*
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            }
        });
        */

        producer.send(record,
            (metadata, exception) -> { // 비동기 동작: 이 부분을 Main Thread가 아닌 다른 스레드가 처리하기 때문
                if (exception == null) {
                    logger.info("""
                            ###### record metadata received #####
                            "topic": {},
                            "partition": {},
                            "offset": {},
                            "timestamp": {},
                            """,
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp()
                    );
                }
                else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            });

        // 메시지를 보내고 조금 기다려야 위에 callback이 실행될 수 있음
        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();
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
