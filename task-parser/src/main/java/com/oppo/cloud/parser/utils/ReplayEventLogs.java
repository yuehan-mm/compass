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
import com.oppo.cloud.parser.domain.reader.ReaderObject;
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
public abstract class ReplayEventLogs {

    // APP 类型
    private ApplicationType applicationType;

    public ReplayEventLogs() {
    }

    public ReplayEventLogs(ApplicationType applicationType) {
        this.applicationType = applicationType;
    }

    public void replay(ReaderObject readerObject) throws Exception {
        try {
            String line;
            while ((line = readerObject.getBufferedReader().readLine()) != null) {
                parseLine(line);
            }
            this.correlate();
        } catch (Exception e) {
            log.warn("replay event log error.  msg: " + e.getMessage());
        } finally {
            readerObject.close();
        }
    }


    /**
     * 按行解析
     */
    abstract void parseLine(String line) throws Exception;

    public abstract void correlate() throws Exception;

    public abstract SparkApplication getApplication();

    public abstract Map<Integer, SparkJob> getJobs();

    public abstract Map<String, SparkExecutor> getExecutors();

    public abstract List<SparkListenerSQLExecutionStart> getSqlExecutionStarts();

    public abstract Map<Long, AccumulableInfo> getAccumulableInfoMap();

    public abstract Map<Long, Long> getDriverUpdateMap();

    public abstract JobFinishedEvent getJobFinishedEvent();

    public abstract MapReduceApplication getMapReduceApplication();
}
