import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {

        String topicName = "simple-topic";

        // KafkaProducer configuration setting
        // null, "hello world"
        Properties props = initProducerProps();

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello world 2");

        // KafkaProducer 메시지 전송
        producer.send(record);

        producer.flush(); // 배치로 처리되기때문에 버퍼를 비워주면서 전송을 강제하기 위함
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
