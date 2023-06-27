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

import com.oppo.cloud.common.constant.LogPathType;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import lombok.Data;

import java.util.*;


@Data
public class App {

    /**
     * appId值[调度日志，appId为空]
     */
    private String appId;
    /**
     * Application 重试次数
     */
    private Integer tryNumber;

    /**
     * driver主机名
     */
    private String amHost;

    /**
     * 日志信息[里面只有一个值]
     */
    private List<LogInfo> logInfoList;

//    public void formatAppLog(TaskApp taskApp) {
//        this.setAppId(taskApp.getApplicationId());
//        this.setTryNumber(taskApp.getRetryTimes());
//        LogInfo logInfo = new LogInfo();
//
//        Map<String, List<LogPath>> logPathMap = new HashMap<>();
//        logPathMap.put("event", Collections.singletonList(new LogPath("hdfs", "event", LogPathType.FILE,
//                taskApp.getEventLogPath())));
//        logPathMap.put("executor", Collections.singletonList(new LogPath("hdfs", "executor", LogPathType.DIRECTORY,
//                taskApp.getYarnLogPath())));
//        logInfo.setLogPathMap(logPathMap);
//        this.setAmHost(taskApp.getAmHost());
//        this.setLogInfoList(Collections.singletonList(logInfo));
//    }

    public void formatSchedulerLog(List<String> schedulerLogPathList, Integer tryNumber) {
        this.setTryNumber(tryNumber);
        LogInfo logInfo = new LogInfo();
        logInfo.setLogGroup("scheduler");
        Map<String, List<LogPath>> logPathMap = new HashMap<>();
        List<LogPath> logPathList = new ArrayList<>();
        for (String schedulerLogPath : schedulerLogPathList) {
            logPathList.add(new LogPath("oss", LogPathType.FILE, schedulerLogPath));
        }
        logPathMap.put("scheduler", logPathList);
        logInfo.setLogPathMap(logPathMap);
        this.setLogInfoList(Collections.singletonList(logInfo));
    }
}
