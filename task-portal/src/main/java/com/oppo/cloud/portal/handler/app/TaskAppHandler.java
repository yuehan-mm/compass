package com.oppo.cloud.portal.handler.app;

import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.portal.service.ElasticSearchService;
import com.oppo.cloud.model.TaskApplication;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/7                                                                                *
 *================================================================================================*/
public interface TaskAppHandler {
  public void handler(TaskApplication taskApplication, TaskApp taskApp, ElasticSearchService elasticSearchService, RedisService redisService) throws Exception;
}
