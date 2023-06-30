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

package com.oppo.cloud.parser.service.job;

import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.domain.job.LogRecord;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.parser.config.CustomConfig;
import com.oppo.cloud.parser.config.ThreadPoolConfig;
import com.oppo.cloud.parser.domain.job.TaskParam;
import com.oppo.cloud.parser.domain.job.TaskResult;
import com.oppo.cloud.parser.service.job.task.Task;
import com.oppo.cloud.parser.service.job.task.TaskFactory;
import com.oppo.cloud.parser.service.writer.ElasticWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Job manager
 */
@Slf4j
@Service
public class JobManager {

    @Resource
    private CustomConfig customConfig;

    @Resource
    private RedisService redisService;

    @Resource(name = ThreadPoolConfig.JOB_THREAD_POOL)
    private Executor jobExecutorPool;

    public JobManager() {

    }

    private List<Task> createTasks(LogRecord logRecord) {
        List<Task> tasks = new ArrayList<>();
        logRecord.getApplications().forEach((appid, taskApp) -> {
            Task task = TaskFactory.create(new TaskParam(taskApp, logRecord.getIsOneClick()));
            if (task == null) {
                log.warn("unsupported log group:{},{}", appid, taskApp);
                return;
            }
            tasks.add(task);
        });
        return tasks;
    }

    public List<CompletableFuture<TaskResult>> createFutures(List<Task> tasks, Executor taskThreadPool) {

        List<CompletableFuture<TaskResult>> futures = new ArrayList<>();

        for (Task task : tasks) {
            CompletableFuture<TaskResult> future = CompletableFuture.supplyAsync(() -> {
                TaskResult taskResult = null;
                try {
                    taskResult = task.run();
                } catch (Exception e) {
                    log.error("Exception:", e);
                }
                return taskResult;
            }, taskThreadPool);
            futures.add(future);
        }

        return futures;
    }

    /***
     *
     * @param logRecord
     * @throws Exception
     */
    public void run(LogRecord logRecord) throws Exception {
        log.info("start job logRecord : {}", JSONObject.toJSONString(logRecord));
        List<Task> tasks = createTasks(logRecord);

        if (tasks.size() == 0) {
            return;
        }

        long start = System.currentTimeMillis();

        List<CompletableFuture<TaskResult>> futures = createFutures(tasks, jobExecutorPool);
        List<TaskResult> taskResults = new ArrayList<>();

        for (Future<TaskResult> result : futures) {
            TaskResult taskResult;
            try {
                taskResult = result.get();
                if (taskResult != null) {
                    taskResults.add(taskResult);
                }
            } catch (Exception e) {
                log.error("Exception:", e);
            }
        }

        try {
            log.info("finish job logRecord : {}", JSONObject.toJSONString(taskResults));
            ElasticWriter.getInstance().saveTaskResults(logRecord, taskResults);
        } catch (Exception e) {
            log.error("Exception:", e);
        }

        long end = System.currentTimeMillis();
        log.info("finished job({}) elapsed time :{}(ms)", logRecord.getId(), end - start);
    }

}