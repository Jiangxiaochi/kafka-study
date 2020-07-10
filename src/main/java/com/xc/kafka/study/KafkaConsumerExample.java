package com.xc.kafka.study;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * 详见
 * http://kafka.apache.org/10/javadoc/?org/apache/kafka/clients/consumer/KafkaConsumer.html
 * https://www.orchome.com/451
 */
public class KafkaConsumerExample {

    public static void main(String[] args) {
        resumeConsumer();
    }

    /**
     * 自动提交offset
     */
    public static void autoCommitOffset() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.32:9092");
        props.put("group.id", "test");

        props.put("enable.auto.commit", "true");//是否允许自动提交
        props.put("auto.commit.interval.ms", "1000");//自动提交频率

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            /**
             * 两种方式指定主题和分区
             */
            //consumer.subscribe(Arrays.asList("my-topic"));
            consumer.assign(Arrays.asList(new TopicPartition("my-topic", 0)));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            }
        }
    }

    /**
     * 手动提交offset
     */
    public static void manualOffset() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.32:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("my-topic"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                /**
                 * operations that could return failed
                 */
                //if(success)
                consumer.commitSync();//手动提交偏移量，如果插入数据库等操作失败则不提交，下一次启动时会从之前的offset开始读取数据
            }
        }
    }

    /**
     * 更精确的指定消费的offset
     */
    public static void manageOffset() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.32:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("my-topic"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        }
    }

    public static void resumeConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.32:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition topicPartition = new TopicPartition("my-topic", 0);
            //手动指定消费位置，无法自动负载均衡，所以要手动注册，才能消费，先assign再seek
            consumer.assign(Arrays.asList(topicPartition));
            //consumer.seekToBeginning(Arrays.asList(topicPartition));
            //consumer.seekToEnd(Arrays.asList(topicPartition));
            consumer.seek(topicPartition, 5);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                consumer.commitSync();//手动提交偏移量，如果插入数据库等操作失败则不提交，下一次启动时会从之前的offset开始读取数据
            }
        }
    }


}
