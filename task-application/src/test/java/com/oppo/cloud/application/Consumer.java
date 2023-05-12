package com.oppo.cloud.application;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","10.163.137.150:9092,10.163.137.151:9092,10.163.137.152:9092");
        props.put("group.id", "test-0925");
//        props.put("enable.auto.commit", "false");
//        props.put("auto.offset.reset", "earliest");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username='hdop' password='v41ieoEFhnPajg5L';");
        @SuppressWarnings("resource")
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test_airflow_cdc_data"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition= %d, offset = %d, key = %s, value = %s\n", record.partition(),
                        record.offset(), record.key(), record.value());
                //consumer.commitSync();
            }
        }
    }
}