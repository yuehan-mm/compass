/*
 * Copyright 2023 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.cloud.syncer.consumer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Properties;

/**
 * 处理日志延迟任务
 */
@Slf4j
@Component
@Configuration
@ConditionalOnProperty(prefix = "custom.delayedTask", name = "enable", havingValue = "true")
public class DelayedCons implements CommandLineRunner {


    @Override
    public void run(String... args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers","10.163.137.150:9092,10.163.137.151:9092,10.163.137.152:9092");
        props.put("group.id", "test_airflow_cdc_data_22047328_d");
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
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                log.error("--------------------= %d, offset = %d, key = %s, value = %s\n", record.partition(),
                        record.offset(), record.key(), record.value());
                //consumer.commitSync();
            }
        }
    }

}
