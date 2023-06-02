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

import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.detect.config.ThreadPoolConfig;
import com.oppo.cloud.detect.domain.DelayedTaskInfo;
import com.oppo.cloud.detect.service.DelayedTaskService;
import com.oppo.cloud.detect.service.DetectService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * 延迟任务定时扫表处理
 */
@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "custom.delayedTaskQueue", name = "enable", havingValue = "true")
public class DelayedTask implements CommandLineRunner {

    @Value("${custom.redis.logRecord}")
    private String logRecordQueue;

    @Value("${custom.redis.delayedQueue}")
    private String delayedQueue;

    @Value("${custom.redis.processing}")
    private String processingKey;

    @Value("${custom.delayedTaskQueue.delayedSeconds}")
    private Integer delaySeconds;

    @Value("${custom.delayedTaskQueue.tryTimes}")
    private Integer tryTimes;

    @Resource(name = ThreadPoolConfig.DELAY_QUEUE_EXECUTOR_POOL)
    private Executor delayQueueExecutorPool;

    @Autowired
    private DelayedTaskService delayedTaskService;

    @Autowired
    private RedisService redisService;

    @Resource
    private List<DetectService> abnormalDetects;

    @PostConstruct
    void init() {
        // 加载因重启而中断的任务
        Map<Object, Object> processingMap = null;
        try {
            processingMap = redisService.hGetAll(processingKey);
        } catch (Exception e) {
            log.error("get processing key err:", e);
        }
        if (processingMap != null) {
            log.info("initProcessingTaskSize:{}", processingMap.size());
            processingMap.forEach((k, v) -> {
                log.info("initProcessingTaskData,k:{},v:{}", k, v);
                redisService.zSetAdd(delayedQueue, v, System.currentTimeMillis());
                redisService.hDel(processingKey, k);
            });
        }
    }

    @Override
    public void run(String... args) throws Exception {
        while (true) {
            try {
                List<DelayedTaskInfo> delayedTaskInfoList = delayedTaskService.getDelayedTasks();
                if (delayedTaskInfoList == null) {
                    Thread.sleep(delaySeconds * 1000);
                    continue;
                }
                for (DelayedTaskInfo delayedTaskInfo : delayedTaskInfoList) {
                    delayQueueExecutorPool.execute(() -> handleDelayTask(delayedTaskInfo));
                }
            } catch (Exception e) {
                log.error("Exception:", e);
            }
        }
    }

    public void handleDelayTask(DelayedTaskInfo delayedTaskInfo) {
        log.info("delayProcessTask:{}", delayedTaskInfo);
        try {
            if (delayedTaskInfo.getProcessRetries() >= tryTimes) {
                redisService.hDel(processingKey, delayedTaskInfo.getKey());
                log.error("delay task retry failed:{}", delayedTaskInfo);
                return;
            }
            if (delayedTaskInfo.getJobAnalysis().getCategories().size() == 0) {
                abnormalDetects.get(0).handleNormalJob(delayedTaskInfo.getJobAnalysis(), delayedTaskInfo.getProcessRetries());
            } else {
                abnormalDetects.get(0).handleAbnormalJob(delayedTaskInfo.getJobAnalysis(), delayedTaskInfo.getProcessRetries());
            }
            redisService.hDel(processingKey, delayedTaskInfo.getKey());
        } catch (Exception e) {
            log.error("dealWithDelayTask failed, msg: ", e);
        }
    }
}
