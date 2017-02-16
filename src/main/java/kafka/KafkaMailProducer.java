package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Kafka Producer
 *
 * @YuChuanQi
 * @since 2016-09-12 17:00:20
 */
public class KafkaMailProducer {
    private static final String topic = "testtopic";
    private static final String brokers = "poc25:9092,poc13:9092";
    private Producer<String, String> producer;

    public KafkaMailProducer() {
        Properties properties = new Properties();
        properties.setProperty("metadata.broker.list", brokers);
        //Producer中没有zookeeper.connect属性,而是在Consumer中
        //properties.put("zookeeper.connect", "poc22");
        properties.setProperty("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<String, String>(config);
    }

    /**
     * 插入一条记录
     */
    public void send(String topic, String message) {
        KeyedMessage<String, String> kms = new KeyedMessage<String, String>(topic, message);
        producer.send(kms);
    }

    public void send(String topic, List<String> messages) {
        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();
        for (String message : messages) {
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic, message);
            kms.add(km);
        }
        producer.send(kms);
    }

    public void readFile(String fileName, String topic) throws Exception{
        BufferedReader br = null;
        try {
            InputStream in = KafkaMailProducer.class.getResourceAsStream(fileName);
            br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            List<String> messages = new ArrayList<String>();
            while ((line = br.readLine()) != null) {
                messages.add(line);
                System.out.println(line);
            }
            send(topic, messages);
        } finally {
            if (br != null) br.close();
        }
    }

    public static void main(String[] args) throws Exception {
        KafkaMailProducer kp = new KafkaMailProducer();
        kp.readFile("/aaa", topic);
    }
}
