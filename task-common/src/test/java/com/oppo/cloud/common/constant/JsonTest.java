package com.oppo.cloud.common.constant;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/15                                                                                *
 *================================================================================================*/
public class JsonTest {
  @Test
  public void test(){
    TestBean bean = new TestBean();
    bean.setApplicationId("1111");
    bean.setApplicationType(ApplicationType.FLINK);

    String message = JSONObject.toJSONString(bean);

    System.out.println(message);
    TestBean testBean = JSONObject.parseObject(message, TestBean.class);
    System.out.println(testBean);
  }
}
