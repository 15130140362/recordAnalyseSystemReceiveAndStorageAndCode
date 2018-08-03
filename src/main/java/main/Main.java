package main;

import com.google.gson.Gson;
import data.Record;
import hbase.HBaseCreateOP;
import hbase.HBaseInsert;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Main {


    private static String key = "rdbb";//redis key.原来的为rdb
    private static String redisHost = "192.168.47.143";
    private static int redisPort = 6379;
    private static String topicName = "fil";
    private static String zookeeperClusterIP = "192.168.47.143:9092";
    private static String recordFilePath = "F:/BaiduNetdiskDownload/record.json";
    private static Jedis jedis = new Jedis(redisHost, redisPort,10000);
    private static HBaseInsert in = new HBaseInsert();
    private static ConsumerConnector consumer;


    static void jsonToKafka() throws IOException {
        Properties props = new Properties();
        //根据这个配置获取metadata,不必是kafka集群上的所有broker,但最好至少有两个
      //  props.put("metadata.broker.list", "192.168.1.104:9092,192.168.1.101:9092");
       // props.put("metadata.broker.list", "192.168.47.140:9092,192.168.47.141:9092");
        props.put("metadata.broker.list", "192.168.47.143:9092");
        //消息传递到broker时的序列化方式
        props.put("serializer.class", StringEncoder.class.getName());
        //zk集群
        props.put("zookeeper.connect", "192.168.47.143:2181");
//        props.put("zookeeper.connect", "192.168.47.140:2181,192.168.47.141:2181");
/*
        props.put("zookeeper.connect", "192.168.1.104:2181,192.168.1.101:2181");
*/
        //是否获取反馈
        //0是不获取反馈(消息有可能传输失败)
        //1是获取消息传递给leader后反馈(其他副本有可能接受消息失败)
        //-1是所有in-sync replicas接受到消息时的反馈
        props.put("request.required.acks", "1");

        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
                new ProducerConfig(props));


        BufferedReader br = new BufferedReader(new FileReader("F:/BaiduNetdiskDownload/record.json"));
        int i = 0;//message key
        String record;
        //send record to kafka
        while ((record = br.readLine()) != null) {
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topicName, Integer.toString(i), record);
            producer.send(keyedMessage);
            i++;
           // System.out.println("1");
        }
        producer.close();
    }

    static void kafkaToRedis() {
        //String zookeeper = "192.168.1.104:2181,192.168.1.101:2181";
//        String zookeeper = "192.168.1.104:2181,192.168.1.101:2181";
        //String zookeeper = "192.168.47.140:2181,192.168.47.141:2181";
        String zookeeper = "192.168.47.143:2181";
        String groupId = "test2";
        Properties props = new Properties();
        Pipeline pipelineq = jedis.pipelined();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "smallest");
        props.put("zookeeper.session.timeout.ms", "6000");
        props.put("zookeeper.sync.time.ms", "200");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("filtered_end", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get("filtered_end").get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        int i=0;
        System.out.println("jjj");
        while (iterator.hasNext()) {
            try {
                MessageAndMetadata<byte[], byte[]> next = iterator.next();
                System.out.println(i++);
                //System.out.println("offset:" + next.offset());
                //System.out.println("key   :" + next.key());
                String message = new String(next.message(), "utf-8");
           //     System.out.println("收到消息" + message);
                pipelineq.sadd(key, message);//record to redis
            } catch (Throwable e) {
                System.out.println(e.getCause());
            }
        }
    }
    static void redisToHbase() throws IOException {
        List list = new ArrayList();
        Record record;
        Gson gson = new Gson();
        long sum=0;
        for (String jsonString : jedis.smembers(key)) {
            System.out.println(sum++);
            record = gson.fromJson(jsonString, Record.class);
            list.add(record);
        }
        in.insertRecordsToHBase(list);
    }

    public static void main(String[] args) throws IOException {


		/*
        1.发送record至kafka
		 */
    //    jsonToKafka();
        /*
        2.将kafka中的信息写入redis
		 */
 //       kafkaToRedis();
		/*
		3.在HBase中创建数据库(创建一次)
		 */
        	//HBaseCreateOP.main(args);
		/*
		4.将redis中的数据发送至HBase
		 */
        	redisToHbase();
    }
}
