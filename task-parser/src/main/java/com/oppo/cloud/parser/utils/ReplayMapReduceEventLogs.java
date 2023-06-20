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

import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.constant.ApplicationType;
import com.oppo.cloud.parser.domain.mapreduce.jobhistory.JobFinishedEvent;
import com.oppo.cloud.parser.domain.mapreduce.jobhistory.MapReduceApplication;
import com.oppo.cloud.parser.domain.spark.eventlog.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * spark event log 解析
 */
@Slf4j
@Data
public class ReplayMapReduceEventLogs extends ReplayEventLogs {

    private MapReduceApplication mapReduceApplication;
    private JobFinishedEvent jobFinishedEvent;

    public ReplayMapReduceEventLogs(ApplicationType applicationType) {
        super(applicationType);
        mapReduceApplication = new MapReduceApplication();
        jobFinishedEvent = new JobFinishedEvent();
    }

    @Override
    public void parseLine(String line) {
        if (line.equals("Avro-Json") || StringUtils.isEmpty(line)) return;
        String type = JSONObject.parseObject(line).getString("type");
        switch (type) {
            case "JOB_FINISHED":
                JSONObject event = JSONObject.parseObject(line).getJSONObject("event");
                JSONObject eventJSONObject = event.getJSONObject("org.apache.hadoop.mapreduce.jobhistory.JobFinished");
                jobFinishedEvent.setFinishedMaps(eventJSONObject.getInteger("finishedMaps"));
                jobFinishedEvent.setFinishedReduces(eventJSONObject.getInteger("finishedReduces"));
                mapReduceApplication.setAppEndTimestamp(eventJSONObject.getLong("finishTime"));
                break;
            case "JOB_SUBMITTED":
                JSONObject jobSubmitEvent = JSONObject.parseObject(line).getJSONObject("event");
                JSONObject jobSubmitEventJSONObject = jobSubmitEvent.getJSONObject("org.apache.hadoop.mapreduce.jobhistory.JobSubmitted");
                mapReduceApplication.setAppStartTimestamp(jobSubmitEventJSONObject.getLong("submitTime"));
                break;
            default:
                break;
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
}
