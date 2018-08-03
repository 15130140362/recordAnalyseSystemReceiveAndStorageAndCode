package kafka;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ProducerExample {
    private String topicName;

    public ProducerExample(String name){
        topicName = name;
    }

    public void sendRecords() throws IOException, InterruptedException {
        Properties props = new Properties();
        //根据这个配置获取metadata,不必是kafka集群上的所有broker,但最好至少有两个
        props.put("metadata.broker.list", "192.168.1.104:9092,192.168.1.101:9092");
        //消息传递到broker时的序列化方式
        props.put("serializer.class", StringEncoder.class.getName());
        //zk集群
        props.put("zookeeper.connect", "192.168.1.104:2181,192.168.1.101:2181");
        //是否获取反馈
        //0是不获取反馈(消息有可能传输失败)
        //1是获取消息传递给leader后反馈(其他副本有可能接受消息失败)
        //-1是所有in-sync replicas接受到消息时的反馈
        props.put("request.required.acks", "1");

        kafka.javaapi.producer.Producer<String , String> producer = new kafka.javaapi.producer.Producer<String , String>(
                new ProducerConfig(props));


        BufferedReader br=new BufferedReader(new FileReader("F:/BaiduNetdiskDownload/123.json"));
        int i = 0;//message key
        String record;
        //send record to kafka
        while((record = br.readLine())!=null) {
            KeyedMessage<String , String> keyedMessage = new KeyedMessage<String , String>(topicName, Integer.toString(i),record);
            producer.send(keyedMessage);
            i++;
        }
        producer.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        new ProducerExample("wochaojishuaide").sendRecords();
    }
}
