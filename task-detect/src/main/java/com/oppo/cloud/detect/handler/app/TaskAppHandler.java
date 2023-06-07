package com.oppo.cloud.detect.handler.app;

import com.oppo.cloud.common.domain.elasticsearch.TaskApp;
import com.oppo.cloud.common.service.RedisService;
import com.oppo.cloud.detect.service.ElasticSearchService;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/7                                                                                *
 *================================================================================================*/
public interface TaskAppHandler {
  public void handler(TaskApp taskApp, ElasticSearchService elasticSearchService, RedisService redisService) throws Exception;
}
