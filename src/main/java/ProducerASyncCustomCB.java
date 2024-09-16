import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncCustomCB { // CB: CallBack

    public static void main(String[] args) {

        String topicName = "multipart-topic";

        // KafkaProducer configuration setting
        // null, "hello world"
        Properties props = initProducerProps();

        // KafkaProducer 객체 생성
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        for (int seq = 0; seq < 20; seq++) {
            // ProducerRecord 객체 생성
            ProducerRecord<Integer, String> record = new ProducerRecord<>(
                topicName,
                seq,
                "hello world " + seq
            );
            Callback customCallback = new CustomCallback(seq);

            producer.send(record, customCallback);
        }

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
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        // props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

}
