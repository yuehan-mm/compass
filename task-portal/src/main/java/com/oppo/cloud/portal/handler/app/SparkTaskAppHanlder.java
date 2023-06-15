package com.oppo.cloud.portal.handler.app;

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
import com.oppo.cloud.model.TaskApplicationExample;
import com.oppo.cloud.portal.service.ElasticSearchService;
import com.oppo.cloud.model.TaskApplication;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/7                                                                                *
 *================================================================================================*/
public class SparkTaskAppHanlder implements TaskAppHandler{
  @Override
  public void handler(TaskApplication taskApplication, TaskApp taskApp, ElasticSearchService elasticSearchService, RedisService redisService) throws Exception {

    SparkApp sparkApp = elasticSearchService.searchSparkApp(taskApplication.getApplicationId());
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
    String attemptId = StringUtils.isNotEmpty(sparkApp.getAttemptId()) ? sparkApp.getAttemptId() : "1";
    String eventLogPath = sparkApp.getEventLogDirectory() + "/" + taskApplication.getApplicationId() + "_" + attemptId;

    taskApp.addLogPath(LogType.SPARK_EVENT, new LogPath("hdfs", LogPathType.FILE, eventLogPath));

    String yarnLogPath = getYarnLogPath(yarnApp.getIp(), redisService);
    if ("".equals(yarnLogPath)) {
        throw new Exception(String.format("can not find yarn log path: rm ip : %s", yarnApp.getIp()));
    }

    taskApp.addLogPath(LogType.CONTAINER, new LogPath("hdfs", LogPathType.FILE, yarnLogPath));
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
