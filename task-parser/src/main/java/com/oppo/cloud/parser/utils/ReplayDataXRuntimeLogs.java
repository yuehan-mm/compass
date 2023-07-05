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

package com.oppo.cloud.parser.utils;

import com.oppo.cloud.common.constant.ApplicationType;
import com.oppo.cloud.parser.domain.mapreduce.eventlog.JobFinishedEvent;
import com.oppo.cloud.parser.domain.mapreduce.eventlog.MapReduceApplication;
import com.oppo.cloud.parser.domain.spark.eventlog.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * spark event log 解析
 */
@Slf4j
@Data
public class ReplayDataXRuntimeLogs extends ReplayEventLogs {

    private JobFinishedEvent jobFinishedEvent;

    public ReplayDataXRuntimeLogs(ApplicationType applicationType, String logPath) {
        super(applicationType, logPath);
        jobFinishedEvent = new JobFinishedEvent();
    }

    @Override
    public void parseLine(String line) {
        try {
            if (line.contains("StandAloneJobContainerCommunicator")) {
                log.info("---------------" + line);
            }
        } catch (Exception e) {
            log.info("ReplayMapReduceEventLogs parse fail. " + line);
            throw e;
        }
    }


    @Override
    public void correlate() throws Exception {
    }

    @Override
    public SparkApplication getApplication() {
        return null;
    }

    @Override
    public Map<Integer, SparkJob> getJobs() {
        return null;
    }

    @Override
    public Map<String, SparkExecutor> getExecutors() {
        return null;
    }

    @Override
    public List<SparkListenerSQLExecutionStart> getSqlExecutionStarts() {
        return null;
    }

    @Override
    public Map<Long, AccumulableInfo> getAccumulableInfoMap() {
        return null;
    }

    @Override
    public Map<Long, Long> getDriverUpdateMap() {
        return null;
    }

    @Override
    public MapReduceApplication getMapReduceApplication() {
        return null;
    }
}
