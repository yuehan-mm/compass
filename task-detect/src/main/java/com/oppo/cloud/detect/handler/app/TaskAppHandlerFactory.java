package com.oppo.cloud.detect.handler.app;

import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.model.TaskApplication;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/8                                                                                *
 *================================================================================================*/
public class TaskAppHandlerFactory {
  public static TaskAppHandler getTaskAppHandler(TaskApplication taskApplication){

    switch (taskApplication.getApplicationType()){
      case SPARK :
        return new SparkTaskAppHanlder();
      case MAPREDUCE:
        return new MRTaskAppHandler();
      default:
        throw new IllegalArgumentException("Invalid taskApp type : " + taskApplication.getApplicationType());
    }
  }
}
