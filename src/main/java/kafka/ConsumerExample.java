package kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerExample {
    private ConsumerConnector consumer;
    private  String topicName;
    public ConsumerExample(){}
    public ConsumerExample(String topic){
        topicName = topic;
    }

    public  void consume(){

        String zookeeper = "192.168.1.104:2181,192.168.1.105:2181";
        String groupId = "test";
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "smallest");
        props.put("zookeeper.session.timeout.ms", "6000");
        props.put("zookeeper.sync.time.ms", "200");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topicName, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topicName).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            try {
                MessageAndMetadata<byte[], byte[]> next = iterator.next();
                System.out.println("offset:" + next.offset());
                System.out.println("key   :"+next.key());
                String message = new String(next.message(),"utf-8");
                System.out.println("收到消息" + message);

            } catch (Throwable e) {
                System.out.println(e.getCause());
            }
        }
    }

    public static void main(String[] args) {
        new ConsumerExample("test").consume();
    }
}
