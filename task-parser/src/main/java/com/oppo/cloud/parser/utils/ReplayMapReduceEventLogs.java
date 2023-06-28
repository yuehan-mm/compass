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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.constant.ApplicationType;
import com.oppo.cloud.common.constant.LogPathType;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.parser.domain.mapreduce.eventlog.JobFinishedEvent;
import com.oppo.cloud.parser.domain.mapreduce.eventlog.MapReduceApplication;
import com.oppo.cloud.parser.domain.spark.eventlog.*;
import com.oppo.cloud.parser.service.reader.IReader;
import com.oppo.cloud.parser.service.reader.LogReaderFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.BufferedReader;
import java.util.*;

/**
 * spark event log 解析
 */
@Slf4j
@Data
public class ReplayMapReduceEventLogs extends ReplayEventLogs {

    private MapReduceApplication mapReduceApplication;
    private JobFinishedEvent jobFinishedEvent;

    public ReplayMapReduceEventLogs(ApplicationType applicationType, String logPath) {
        super(applicationType, logPath);
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
                mapReduceApplication.setJobConfiguration(getJobConfiguration(eventJSONObject.getString("jobid")));
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

    private Properties getJobConfiguration(String jobId) {
        Properties properties = new Properties();
        String jobConfPath = this.getLogPath().toString()
                .substring(0, this.getLogPath().lastIndexOf("/") + 1) + jobId + "_conf.xml";
        log.info("job conf path : " + jobConfPath);
        try {
            IReader reader = LogReaderFactory.create(new LogPath("hdfs", LogPathType.FILE, jobConfPath));
            copyProperties(reader.getReaderObject().getBufferedReader(), properties);
            log.debug("JobConfiguration : " + JSON.toJSONString(properties));
        } catch (Exception e) {
            log.error("getJobConfiguration error : ", e);
        }
        return properties;
    }

    public static void copyProperties(BufferedReader bufferedReader, Properties properties) {
        try {
            // 创建SAXReader对象
            SAXReader reader = new SAXReader();
            // 加载xml文件
            Document dc = reader.read(bufferedReader);
            // 获取迭代器
            Iterator it = dc.getRootElement().elementIterator();
            // 遍历迭代器，获取根节点信息
            while (it.hasNext()) {
                Element book = (Element) it.next();
                String key = book.element("name").getText();
                if (key.equals("yarn.app.mapreduce.am.resource.mb")
                        || key.equals("mapreduce.reduce.memory.mb")
                        || key.equals("mapreduce.map.memory.mb")) {
                    properties.put(key, book.element("value").getText());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
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
