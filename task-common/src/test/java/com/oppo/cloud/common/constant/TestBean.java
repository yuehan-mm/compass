package com.oppo.cloud.common.constant;

import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/15                                                                                *
 *================================================================================================*/
@Data
public class TestBean {
  private String applicationId;

  @JSONField(serializeFeatures= JSONWriter.Feature.WriteEnumsUsingName)
  private ApplicationType applicationType;

//  @Override
//  public String toString() {
//    return "TestBean{" +
//            "applicationId='" + applicationId + '\'' +
//            ", applicationType=" + applicationType +
//            '}';
//  }
}
