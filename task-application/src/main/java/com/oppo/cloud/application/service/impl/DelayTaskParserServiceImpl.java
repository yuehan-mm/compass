package com.oppo.cloud.application.service.impl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.oppo.cloud.application.constant.RetCode;
import com.oppo.cloud.application.constant.RetryException;
import com.oppo.cloud.application.domain.DelayedTaskInfo;
import com.oppo.cloud.application.domain.ParseRet;
import com.oppo.cloud.application.service.DelayTaskParserService;
import com.oppo.cloud.application.service.DelayedTaskService;
import com.oppo.cloud.application.service.LogParserService;
import com.oppo.cloud.common.domain.syncer.TableMessage;
import com.oppo.cloud.model.TaskInstance;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SimpleThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.*;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  延迟计算触发逻辑                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/5/30                                                                                *
 *================================================================================================*/
@Service
@Slf4j
public class DelayTaskParserServiceImpl implements DelayTaskParserService {

  private static final long DELAY_TIME = 5 * 60 * 1000;
  private static final int CONTAINER_SIZE = 2000;
  private static final String TASK_TYPE_FLINK = "FLINK";

  private BlockingQueue<TaskInstance> taskInstanceQueue = new PriorityBlockingQueue<>(CONTAINER_SIZE);
  private ExecutorService workExecutor = Executors.newSingleThreadExecutor();

  @Autowired
  private LogParserService logParserService;

  @Autowired
  private DelayedTaskService delayedTaskService;

  public DelayTaskParserServiceImpl(){
    workExecutor.submit(new TaskParserWorker());
  }

  @Override
  public void handle(TaskInstance taskInstance) throws Exception {

    if(!taskInstance.isFinish() &&  !taskInstance.getTaskType().equals(TASK_TYPE_FLINK)) return;

    log.debug("Add new task instance : {}", taskInstance);
    taskInstanceQueue.add(taskInstance);
  }

  public void setLogParserService(LogParserService logParserService) {
    this.logParserService = logParserService;
  }

  public void setDelayedTaskService(DelayedTaskService delayedTaskService) {
    this.delayedTaskService = delayedTaskService;
  }


  private class TaskParserWorker implements Runnable{

    @SneakyThrows
    @Override
    public void run() {

      while (true){

        TaskInstance taskInstance = taskInstanceQueue.take();


        long delayTime = DELAY_TIME - (DateTime.now().getMillis() - taskInstance.getFinishTime());

        // 延迟处理
        if (delayTime > 0){
          Thread.sleep(delayTime);
        }

        log.debug("Deal with task instance : {}", taskInstance);

        try {
          logParserService.handle(taskInstance);
        }catch (RetryException e){
          delayedTaskService.pushDelayedQueue(new DelayedTaskInfo(UUID.randomUUID().toString(), 1, taskInstance, null));
        }catch (Exception e){
          log.error(e.getMessage());
        }
      }
    }
  }

}
