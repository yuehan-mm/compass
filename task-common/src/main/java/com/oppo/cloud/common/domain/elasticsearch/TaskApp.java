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

package com.oppo.cloud.common.domain.elasticsearch;

import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.annotation.JSONField;
import com.google.common.collect.Lists;
import com.oppo.cloud.common.constant.ApplicationType;
import com.oppo.cloud.common.constant.LogType;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.common.util.DateUtil;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

@Data
public class TaskApp extends EsInfo {

    @ApiModelProperty(value = "applicationId名称")
    private String applicationId;

    @ApiModelProperty(value = "applicationType类型")
    @JSONField(serializeFeatures= JSONWriter.Feature.WriteEnumsUsingName)
    private ApplicationType applicationType;

    @ApiModelProperty(value = "执行用户")
    private String executeUser = "None";

    @ApiModelProperty(value = "执行队列")
    private String queue = "None";

    @ApiModelProperty(value = "集群名称")
    private String clusterName = "None";

    @ApiModelProperty(value = "用户列表")
    private List<SimpleUser> users = Lists.newArrayList();

    @ApiModelProperty(value = "项目名称")
    private String projectName;

    @ApiModelProperty(value = "项目ID")
    private Integer projectId;

    @ApiModelProperty(value = "工作流名称")
    private String flowName;

    @ApiModelProperty(value = "工作流Id")
    private Integer flowId;

    @ApiModelProperty(value = "任务名称")
    private String taskName;

    @ApiModelProperty(value = "任务Id")
    private Integer taskId;

    @ApiModelProperty(value = "执行周期")
    private Date executionDate;

    @ApiModelProperty(value = "任务开始执行时间")
    private Date startTime;

    @ApiModelProperty(value = "任务结束时间")
    private Date finishTime;

    @ApiModelProperty(value = "任务执行耗时")
    private Double elapsedTime;

    @ApiModelProperty(value = "任务执行状态")
    private String taskAppState;

    @ApiModelProperty(value = "执行消耗memory·seconds[mb·s]")
    private Double memorySeconds = 0.0;

    @ApiModelProperty(value = "执行消耗vcore·seconds")
    private Double vcoreSeconds = 0.0;

    @ApiModelProperty(value = "am异常信息")
    private String diagnostics;

    @ApiModelProperty(value = "第几次重试的下的app")
    private Integer retryTimes;

    @ApiModelProperty(value = "异常类型")
    private List<String> categories;

    @ApiModelProperty(value = "任务处理状态")
    private Integer taskStatus = 0;

    @ApiModelProperty(value = "诊断结果")
    private String diagnoseResult;

    @ApiModelProperty(value = "am 主机名")
    private String amHost = "";

    @ApiModelProperty(value = "是否删除")
    private Integer deleted = 0;

    @ApiModelProperty(value = "创建时间")
    private Date createTime;

    @ApiModelProperty(value = "修改时间")
    private Date updateTime;

    private Map<LogType, List<LogPath>> logPaths = new HashMap<>();

    public Map<String, Object> genDoc() throws Exception {
        Map<String, Object> res = new HashMap<>();
        Field[] fileds = this.getClass().getDeclaredFields();
        for (Field field : fileds) {
            String key = field.getName();
            String method = key.substring(0, 1).toUpperCase() + key.substring(1);
            Method getMethod = this.getClass().getMethod("get" + method);
            switch (field.getName()) {
                case "docId":
                    break;
                case "executionDate":
                case "startTime":
                case "finishTime":
                case "updateTime":
                case "createTime":
                    Date value = (Date) getMethod.invoke(this);
                    if (value != null) {
                        res.put(key, DateUtil.timestampToUTCDate(value.getTime()));
                    }
                    break;
                default:
                    res.put(key, getMethod.invoke(this));
            }
        }
        return res;
    }



    public void addLogPath(LogType logType, LogPath logPath){
        if(!this.logPaths.containsKey(logType)){
            this.logPaths.put(logType, new LinkedList<>());
        }

        List<LogPath> logPathList = this.logPaths.get(logType);
        logPathList.add(logPath);
    }

    public String genIndex(String baseIndex) {
        return StringUtils.isNotBlank(this.getIndex()) ? this.getIndex()
                : baseIndex + "-" + DateUtil.format(this.getExecutionDate(), "yyyy.MM.dd");
    }

    public String genDocId() {
        return StringUtils.isNotBlank(this.getDocId()) ? this.getDocId() : UUID.randomUUID().toString();
    }

}
