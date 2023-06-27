package com.oppo.cloud.application.task;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/15                                                                                *
 *================================================================================================*/
public class MRLoggerParserTest {

  @Test
  public void testLogger() throws IOException {
    final Pattern pattern = Pattern.compile(".*The url to Track the job: .*?(?<applicationId>application_[0-9]+_[0-9]+).*");

    BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("mr.log")));

    for(String strLine = reader.readLine(); strLine != null; strLine = reader.readLine()){
      Matcher matcher = pattern.matcher(strLine);
      if(matcher.find()){
        System.out.println(strLine);
      }
    }
    System.out.println("===");

  }
}
