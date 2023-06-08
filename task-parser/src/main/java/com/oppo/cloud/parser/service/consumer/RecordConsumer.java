package com.oppo.cloud.parser.service.consumer;

import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.domain.job.LogRecord;
import com.oppo.cloud.parser.config.CustomConfig;
import com.oppo.cloud.parser.config.ThreadPoolConfig;
import com.oppo.cloud.parser.service.job.JobManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/8                                                                                *
 *================================================================================================*/
@Slf4j
@Component
public class RecordConsumer {

  @Resource
  private CustomConfig config;

  @Resource(name = ThreadPoolConfig.CONSUMER_THREAD_POOL)
  private Executor consumerExecutorPool;


  private Semaphore semaphore = null;

  @Resource
  private JobManager jobManager;

  @KafkaListener(topics = "${spring.kafka.mysqldatatopics}", containerFactory = "kafkaListenerContainerFactory")
  public void receive(@Payload String message,
                      @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                      Consumer consumer) {
    try{
      if(semaphore == null){
        semaphore = new Semaphore(config.getMaxThreadPoolSize());
      }

      semaphore.acquire();
      LogRecord logRecord = JSONObject.parseObject(message, LogRecord.class);
      consumerExecutorPool.execute(() -> consume(logRecord, semaphore));
    }catch (Exception e){
      log.error("consume log record error : {}", e);
    }

  }

  private void consume(LogRecord logRecord, Semaphore semaphore) {
    try {
      jobManager.run(logRecord);
    } catch (Exception e) {
      log.error("Exception:", e);
    }finally {
      semaphore.release();
    }

  }


}
