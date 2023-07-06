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

package com.oppo.cloud.detect.service;

import com.oppo.cloud.common.domain.elasticsearch.JobAnalysis;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.detect.domain.AbnormalTaskAppInfo;

import java.util.List;

/**
 * 任务taskApp接口
 */
public interface TaskAppService {

    AbnormalTaskAppInfo getTaskAppsInfo(JobAnalysis jobAnalysis);

    /**
     * 从Es中获取异常任务的app信息
     */
    List<TaskApp> searchTaskApps(JobAnalysis jobAnalysis) throws Exception;
}
