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

package com.oppo.cloud.application;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 日志解析服务
 * <p>
 * <p>
 * -- 请求hdfs 路径
 * /flume/airflow/dag_id=JOB_COMPASS_HIVE_03/run_id=scheduled__2023-04-14T05_48_00+00_00/task_id=COMPASS_HIVE_03/attempt=1
 * <p>
 * -- 实际存储路径？
 * /bigdata/bdp_jt/airflow/logs/JOB_COMPASS_HIVE_02/COMPASS_HIVE_02/2023-04-14T05:50:00+00:00-1.log
 * /flume/airflow/作业名称/运行时间/任务名称/重试次数
 */
public class LogParserServiceImpl222 {
    private static Map<String, Object> data = new HashMap<>();

    static {
        data.put("flow_name", "JOB_COMPASS_HIVE_02");
        data.put("run_id", "2023-04-14T07_40_00_00_00");
        data.put("task_name", "COMPASS_HIVE_02");
        data.put("retry_times", "1");

    }

    public static void main(String[] args) {
        System.out.println(String.format("%s*", "/flume/airflow/dag_id=JOB_COMPASS_HIVE_03/run_id=scheduled__2023-04-14T05_48_00+00_00/task_id=COMPASS_HIVE_03/attempt=1*"));
        List<String> paths = new ArrayList<>();
        List<LogPathJoin> logPathJoins = buildLogPathjon();
        for (LogPathJoin logPathJoin : logPathJoins) {
            if (StringUtils.isBlank(logPathJoin.getColumn())) {
                paths.add(logPathJoin.getData());
            }



        }
        paths.add(data.get("flow_name").toString());
        paths.add(data.get("task_name").toString());
        paths.add(data.get("run_id") + "-" + data.get("retry_times"));
        System.out.println(String.join("/", paths));

    }


    public static List<LogPathJoin> buildLogPathjon() {


//       - { "column": "run_id",data: "run_id=", "regex": "(?<runId>.*)", "name": "runId" }
//       - { "column": "task_name",data: "task_id=","regex": "(?<taskName>.*)", "name": "taskName" }
//       - { "column": "retry_times",data: "attempt=", "regex": "(?<fileName>.*)", "name": "fileName"
        ArrayList<LogPathJoin> objects = new ArrayList<>();
        objects.add(new LogPathJoin("", "", "", "/flume/airflow/bigdata/bdp_jt/airflow/logs"));
        objects.add(new LogPathJoin("flow_name", "(?<flowName>.*)", "flowName", ""));

        objects.add(new LogPathJoin("task_name", "(?<taskName>.*)", "taskName", ""));
        objects.add(new LogPathJoin("run_id", "(?<runId>.*)", "runId", ""));
        objects.add(new LogPathJoin("retry_times", "(?<fileName>.*)", "fileName", ""));
        return objects;
    }

    static class LogPathJoin {
        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public String getRegex() {
            return regex;
        }

        public void setRegex(String regex) {
            this.regex = regex;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        /**
         * 依赖的数据列
         */
        private String column;
        /**
         * 解析日志列正则
         */
        private String regex;
        /**
         * 匹配日志名称
         */
        private String name;
        /**
         * 静态数据，不需要正则匹配
         */
        private String data;

        public LogPathJoin(String column, String regex, String name, String data) {
            this.column = column;
            this.regex = regex;
            this.name = name;
            this.data = data;
        }
    }

}
