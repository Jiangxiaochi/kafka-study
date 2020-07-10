package com.xc.kafka.study;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaProducerExample {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.2.32:9092");
        properties.put("acks", "all");//是判别请求是否为完整的条件（就是是判断是不是成功发送了）。我们指定了“all”将会阻塞消息，这种设置性能最低，但是是最可靠的。
        properties.put("retries", 0);//如果请求失败，生产者会自动重试，如果启用重试，则会有重复消息的可能性。
        properties.put("batch.size", 16384);//缓存每个分区未发送的消息的缓存大小，较大的缓存会需要更多的内存
        properties.put("linger.ms", 1);//发送请求间隔时间
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//ByteArraySerializaer 序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<String, String>(properties)) {
            //key value partition timestamp
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<String, String>("my-topic", 1, "test", "hhh"), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        } else {
                            System.out.println(recordMetadata);
                        }
                    }
                });
            }
        }

    }

}
