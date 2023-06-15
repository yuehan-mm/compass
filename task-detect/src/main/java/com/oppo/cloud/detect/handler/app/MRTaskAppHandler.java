package com.oppo.cloud.detect.handler.app;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.constant.Constant;
import com.oppo.cloud.common.constant.LogPathType;
import com.oppo.cloud.common.constant.LogType;
import com.oppo.cloud.common.domain.cluster.spark.SparkApp;
import com.oppo.cloud.common.domain.cluster.yarn.YarnApp;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.detect.service.ElasticSearchService;
import com.oppo.cloud.model.TaskApplication;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/7                                                                                *
 *================================================================================================*/
public class MRTaskAppHandler implements TaskAppHandler {
  @Override
  public void handler(TaskApplication taskApplication, TaskApp taskApp, ElasticSearchService elasticSearchService, RedisService redisService) throws Exception {

    YarnApp yarnApp = elasticSearchService.searchYarnApp(taskApplication.getApplicationId());

    taskApp.setStartTime(new Date(yarnApp.getStartedTime()));
    taskApp.setFinishTime(new Date(yarnApp.getFinishedTime()));
    taskApp.setElapsedTime((double) yarnApp.getElapsedTime());
    taskApp.setClusterName(yarnApp.getClusterName());
    taskApp.setApplicationType(taskApplication.getApplicationType());
    taskApp.setQueue(yarnApp.getQueue());
    taskApp.setDiagnostics(yarnApp.getDiagnostics());
    taskApp.setDiagnoseResult(StringUtils.isNotBlank(yarnApp.getDiagnostics()) ? "abnormal" : "");
    taskApp.setCategories(StringUtils.isNotBlank(yarnApp.getDiagnostics())
            ? Collections.singletonList(AppCategoryEnum.OTHER_EXCEPTION.getCategory())
            : new ArrayList<>());
    taskApp.setExecuteUser(yarnApp.getUser());
    taskApp.setVcoreSeconds((double) yarnApp.getVcoreSeconds());
    taskApp.setTaskAppState(yarnApp.getFinalStatus());
    taskApp.setMemorySeconds((double) Math.round(yarnApp.getMemorySeconds()));

    String[] amHost = yarnApp.getAmHostHttpAddress().split(":");
    if (amHost.length == 0) {
      throw new Exception(String.format("parse amHost error, amHost:%s", yarnApp.getAmHostHttpAddress()));
    }

    taskApp.setAmHost(amHost[0]);

    String yarnLogPath = getYarnLogPath(yarnApp.getIp(), redisService);
    if ("".equals(yarnLogPath)) {
      throw new Exception(String.format("can not find yarn log path: rm ip : %s", yarnApp.getIp()));
    }

    yarnLogPath = yarnLogPath + "/" + yarnApp.getUser() + "/logs/" + taskApplication.getApplicationId();

    taskApp.addLogPath(LogType.CONTAINER, new LogPath("hdfs", LogPathType.FILE, yarnLogPath));


    DateTime dirDate = new DateTime(yarnApp.getFinishedTime());
    String[] appArray = taskApplication.getApplicationId().split("_");
    String path = String.format("/user/history/done/%4d/%2d/%2d/0000%s/job_%s_%s",
            dirDate.getYear(),
            dirDate.getMonthOfYear(),
            dirDate.getDayOfMonth(),
            appArray[2].substring(0, 2),
            appArray[1],
            appArray[2]);

    taskApp.addLogPath(LogType.MAPREDUCE_EVENT, new LogPath("hdfs", LogPathType.FILE, path));

  }

  /**
   * 查询redis,获取yarn 日志路径
   */
  public String getYarnLogPath(String rmIp, RedisService redisService) throws Exception {
    if (redisService.hasKey(Constant.RM_JHS_MAP)) {
      Map<String, String> rmJhsMap = JSON.parseObject((String) redisService.get(Constant.RM_JHS_MAP),
              new TypeReference<Map<String, String>>() {
              });
      String jhsIp = rmJhsMap.get(rmIp);
      String key = Constant.JHS_HDFS_PATH + jhsIp;
      if (redisService.hasKey(key)) {
        return (String) redisService.get(key);
      } else {
        throw new Exception(String.format("search redis error,msg: can not find key %s, rmJhsMap:%s, rmIp:%s", key, rmJhsMap, rmIp));
      }

    } else {
      throw new Exception(String.format("search redis error,msg: can not find key %s", Constant.RM_JHS_MAP));
    }
  }
}
