package com.oppo.cloud.parser.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/8                                                                                *
 *================================================================================================*/
@Configuration
@EnableKafka
public class KafkaConfig {
  /**
   * 消费主题
   */
  @Value("${spring.kafka.taskrecord.topic}")
  private String topics;
  /**
   * 消费组
   */
  @Value("${spring.kafka.taskrecord.groupid}")
  private String groupId;
  /**
   * 消费模式: latest、earliest
   */
  @Value("${spring.kafka.consumer.auto-offset-reset}")
  private String autoOffsetReset;
  /**
   * 消费Kafka集群地址
   */
  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;
  /**
   * 消费者自动提交
   */
  @Value("${spring.kafka.consumer.enable-auto-commit}")
  private String enableAutoCommit;
  /**
   * 两次消费最大间隔时间
   */
  @Value("${spring.kafka.consumer.max-poll-interval-ms}")
  private String maxPollIntervalMs;


  @Value("${spring.kafka.consumer.security-protocol}")
  private String securityprotocol;


  @Value("${spring.kafka.consumer.sasl-mechanism}")
  private String saslmechanism;

  @Value("${spring.kafka.consumer.sasl-jaas-config}")
  private String sasljaasconfig;
  /**
   * 最大消费数量
   */

  /**
   * 创建消费者
   */
  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfig(), new StringDeserializer(), new StringDeserializer());
  }
  /**
   * 消费者配置
   */
  public Map<String, Object> consumerConfig() {
    Map<String, Object> config = new HashMap();
    config.put("security.protocol", securityprotocol);
    config.put("sasl.mechanism", saslmechanism);
    config.put("sasl.jaas.config", sasljaasconfig);
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
    config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
    config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

    return config;
  }

  /**
   * 并发消费
   */
  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }
}
