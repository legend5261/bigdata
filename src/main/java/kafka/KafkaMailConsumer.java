package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.HashMap;

/**
 * Created by pc on 2016/9/12.
 */
public class KafkaMailConsumer {
    private static final String zk = "poc12:2181,poc13:2181,poc16:2181,poc22:2181,poc25:2181,";
    private static final String topic = "testtopic";
    private ConsumerConnector consumer;

    public KafkaMailConsumer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zk);
        properties.put("group.id", "test-topic");
        properties.put("zookeeper.session.timeout.ms", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "2000");
        ConsumerConfig config = new ConsumerConfig(properties);
        consumer = Consumer.createJavaConsumerConnector(config);
    }

    public void consumer(String topic, Integer partition) {
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, partition);
        List<KafkaStream<byte[], byte[]>> kafkaStreams = consumer.createMessageStreams(topicMap).get(topic);
        for (KafkaStream<byte[], byte[]> stream : kafkaStreams) {
            while (stream.iterator().hasNext()) {
                String line = new String(stream.iterator().next().message());
                System.out.println(getClass().getSimpleName() + " read message " + line);
            }
        }

    }

    public void close() {
        consumer.shutdown();
    }

    public static void main(String[] args) {
        KafkaMailConsumer consumer = new KafkaMailConsumer();
        consumer.consumer(topic, 1);
        consumer.close();
    }
}
