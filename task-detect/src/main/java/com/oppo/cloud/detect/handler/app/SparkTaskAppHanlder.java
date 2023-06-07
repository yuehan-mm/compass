package com.oppo.cloud.detect.handler.app;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.constant.Constant;
import com.oppo.cloud.common.constant.LogPathType;
import com.oppo.cloud.common.domain.cluster.spark.SparkApp;
import com.oppo.cloud.common.domain.cluster.yarn.YarnApp;
import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.domain.job.LogPath;
import com.oppo.cloud.common.service.RedisService;
import org.apache.commons.lang3.StringUtils;
import com.oppo.cloud.detect.service.ElasticSearchService;

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
public class SparkTaskAppHanlder implements TaskAppHandler{
  @Override
  public void handler(TaskApp taskApplication, ElasticSearchService elasticSearchService, RedisService redisService) throws Exception {

    SparkApp sparkApp = elasticSearchService.searchSparkApp(taskApplication.getApplicationId());
    YarnApp yarnApp = elasticSearchService.searchYarnApp(taskApplication.getApplicationId());

    taskApplication.setStartTime(new Date(yarnApp.getStartedTime()));
    taskApplication.setFinishTime(new Date(yarnApp.getFinishedTime()));
    taskApplication.setElapsedTime((double) yarnApp.getElapsedTime());
    taskApplication.setClusterName(yarnApp.getClusterName());
    taskApplication.setApplicationType(yarnApp.getApplicationType());
    taskApplication.setQueue(yarnApp.getQueue());
    taskApplication.setDiagnostics(yarnApp.getDiagnostics());
    taskApplication.setDiagnoseResult(StringUtils.isNotBlank(yarnApp.getDiagnostics()) ? "abnormal" : "");
    taskApplication.setCategories(StringUtils.isNotBlank(yarnApp.getDiagnostics())
            ? Collections.singletonList(AppCategoryEnum.OTHER_EXCEPTION.getCategory())
            : new ArrayList<>());
    taskApplication.setExecuteUser(yarnApp.getUser());
    taskApplication.setVcoreSeconds((double) yarnApp.getVcoreSeconds());
    taskApplication.setTaskAppState(yarnApp.getFinalStatus());
    taskApplication.setMemorySeconds((double) Math.round(yarnApp.getMemorySeconds()));

    String[] amHost = yarnApp.getAmHostHttpAddress().split(":");
    if (amHost.length == 0) {
      throw new Exception(String.format("parse amHost error, amHost:%s", yarnApp.getAmHostHttpAddress()));
    }

    taskApplication.setAmHost(amHost[0]);
    String attemptId = StringUtils.isNotEmpty(sparkApp.getAttemptId()) ? sparkApp.getAttemptId() : "1";
    String eventLogPath = sparkApp.getEventLogDirectory() + "/" + taskApplication.getApplicationId() + "_" + attemptId;

    taskApplication.addLogPath("event", new LogPath("hdfs", "event", LogPathType.FILE, eventLogPath));

    String yarnLogPath = getYarnLogPath(yarnApp.getIp(), redisService);
    if ("".equals(yarnLogPath)) {
        throw new Exception(String.format("can not find yarn log path: rm ip : %s", yarnApp.getIp()));
    }

    taskApplication.addLogPath("executor", new LogPath("hdfs", "event", LogPathType.FILE, yarnLogPath));
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
