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

package com.oppo.cloud.detect.task;

import com.alibaba.fastjson2.JSON;
import com.oppo.cloud.common.constant.SchedulerType;
import com.oppo.cloud.common.constant.TaskStateEnum;
import com.oppo.cloud.common.domain.elasticsearch.JobAnalysis;
import com.oppo.cloud.common.domain.syncer.TableMessage;
import com.oppo.cloud.common.util.ui.TryNumberUtil;
import com.oppo.cloud.detect.config.ThreadPoolConfig;
import com.oppo.cloud.detect.service.BlocklistService;
import com.oppo.cloud.detect.service.DetectService;
import com.oppo.cloud.detect.service.TaskInstanceService;
import com.oppo.cloud.model.TaskInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * 任务诊断
 */
@Component
@Slf4j
public class DetectedTask {

    @Value("${custom.schedulerType}")
    private String schedulerType;
    @Resource
    private List<DetectService> abnormalDetects;

    @Resource(name = ThreadPoolConfig.DETECT_EXECUTOR_POOL)
    private Executor detectExecutorPool;

    @Autowired
    private TaskInstanceService taskInstanceService;

    @Autowired
    private BlocklistService blocklistService;


    /**
     * 日志消费
     */
    @KafkaListener(topics = "${spring.kafka.taskinstanceapplicationtopics}", containerFactory = "kafkaListenerContainerFactory")
    public void receive(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, Consumer consumer,
                        Acknowledgment ack) throws Exception {
        try {
            log.info("message:{}", message);
            TaskInstance taskInstance = JSON.parseObject(message, TaskInstance.class);
            detectExecutorPool.execute(() -> detectTask(taskInstance));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        ack.acknowledge();
    }


    /**
     * 对每个任务进行诊断
     */
    public void detectTask(TaskInstance taskInstance) {

        if (taskInstance.getProjectName() == null || taskInstance.getFlowName() == null) {
            log.warn("instance projectName or flowName is null:{}", taskInstance);
            return;
        }

        // 过滤白名单任务
        if (blocklistService.isBlocklistTask(taskInstance.getProjectName(),
                taskInstance.getFlowName(),
                taskInstance.getTaskName())
        ) {
            log.info("find blocklist task, taskInstance:{}", taskInstance);
            return;
        }

        JobAnalysis jobAnalysis = new JobAnalysis();
        TaskInstance taskInstanceSum;

        if ("manual".equals(taskInstance.getTriggerType())) {
            // 手动执行的重试当成单次执行周期
            taskInstance.setRetryTimes(0);
            taskInstance.setMaxRetryTimes(0);
            taskInstanceSum = taskInstance;
        } else {
            // 更新任务的开始/结束时间
            taskInstanceSum = taskInstanceService.searchTaskSum(taskInstance.getProjectName(),
                    taskInstance.getFlowName(),
                    taskInstance.getTaskName(),
                    taskInstance.getExecutionTime()
            );
        }

        try {
            BeanUtils.copyProperties(taskInstanceSum, jobAnalysis);
        } catch (Exception e) {
            log.error("taskInstanceSum:{}, taskInstance:{}, exception:{}", taskInstanceSum, taskInstance, e.getMessage());
            return;
        }
        jobAnalysis.setExecutionDate(taskInstanceSum.getExecutionTime());
        jobAnalysis.setDuration((double) (taskInstanceSum.getEndTime().getTime() / 1000
                - taskInstanceSum.getStartTime().getTime() / 1000));
        jobAnalysis.setCategories(new ArrayList<>());

        jobAnalysis.setRetryTimes(TryNumberUtil.updateTryNumber(jobAnalysis.getRetryTimes(),schedulerType));

        // 调度作业级别任务异常检测
        for (DetectService detectService : abnormalDetects) {
            try {
                detectService.detect(jobAnalysis);
            } catch (Exception e) {
                log.error("detect task failed: ", e);
            }
        }

        try {
            if (jobAnalysis.getCategories().size() == 0) {
                // 正常作业任务处理
                abnormalDetects.get(0).handleNormalJob(jobAnalysis, 0);
            } else {
                // 异常作业任务处理
                abnormalDetects.get(0).handleAbnormalJob(jobAnalysis, 0);
            }
        } catch (Exception e) {
            log.error("handle job failed: ", e);
        }

    }
}
