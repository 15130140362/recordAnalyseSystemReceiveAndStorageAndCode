package main;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.kafka.streams.processor.TimestampExtractor;

public  class MyEventTimeExtractor implements TimestampExtractor{


    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord) {
        return System.currentTimeMillis();
    }
}