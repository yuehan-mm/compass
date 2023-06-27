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

package com.oppo.cloud.parser.service.job.task;

import com.oppo.cloud.common.util.spring.SpringBeanUtil;
import com.oppo.cloud.parser.config.ThreadPoolConfig;
import com.oppo.cloud.parser.domain.job.CommonResult;
import com.oppo.cloud.parser.domain.job.TaskParam;
import com.oppo.cloud.parser.domain.job.TaskResult;
import com.oppo.cloud.parser.service.job.parser.IParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Scheduler task
 */
@Slf4j
public class SchedulerTask extends Task {

    private Executor taskThreadPool;

    private final TaskParam taskParam;

    public SchedulerTask(TaskParam taskParam) {
        super(taskParam);
        this.taskParam = taskParam;
        taskThreadPool = (ThreadPoolTaskExecutor) SpringBeanUtil.getBean(ThreadPoolConfig.TASK_THREAD_POOL);
    }

    @Override
    public TaskResult run() {
        List<IParser> parsers = super.createTasks();
        if (parsers.size() == 0) {
            return null;
        }
        List<CompletableFuture<CommonResult>> futures = super.createFutures(parsers, taskThreadPool);

        TaskResult taskResult = new TaskResult();
        taskResult.setAppId(this.taskParam.getTaskApp().getApplicationId());

        List<String> categories = new ArrayList<>();
        for (Future<CommonResult> result : futures) {
            CommonResult commonResult;
            try {
                commonResult = result.get();
                if (commonResult != null) {
                    switch (commonResult.getLogType()) {
                        case SCHEDULER:
                            categories.addAll((List<String>) commonResult.getResult());
                            break;
                        default:
                            break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception:", e);
            }
        }
        taskResult.setCategories(categories);
        return taskResult;

    }

}
