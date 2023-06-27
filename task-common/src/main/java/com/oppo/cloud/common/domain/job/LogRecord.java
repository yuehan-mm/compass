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

package com.oppo.cloud.common.domain.job;

import com.oppo.cloud.common.domain.elasticsearch.JobAnalysis;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务记录
 */
@Data
public class LogRecord {

    private String id;

    //作业信息
    private JobAnalysis jobAnalysis;

    // taskApp信息: Map<applicationId,taskApp>
    private Map<String, TaskApp> applications = new HashMap<>();

    private Boolean isOneClick = false;

    private Long firstConsumeTime = System.currentTimeMillis();

    private Integer consumeCount = 0;

    public void formatTaskAppList(List<TaskApp> taskAppList) {
        taskAppList.stream()
                .forEach(taskApp -> applications.put(taskApp.getApplicationId(), taskApp));
    }

}
