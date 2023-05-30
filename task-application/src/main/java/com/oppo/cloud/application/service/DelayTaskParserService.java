package com.oppo.cloud.application.service;

import com.oppo.cloud.common.domain.syncer.TableMessage;


/**************************************************************************************************
 * <pre>                                                                                          *
 *  基于延迟的日志解析服务                                                                           *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                               *
 * @date : 2023/5/30                                                                              *
 *================================================================================================*/
public interface DelayTaskParserService {
  public void handle(TableMessage tableMessage) throws Exception;
}