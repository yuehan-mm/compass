package com.oppo.cloud.application.constant;

import java.io.IOException;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/5/31                                                                                *
 *================================================================================================*/
public class RetryException extends Exception {
  static final long serialVersionUID = 3700124864236090155L;


  public RetryException() {
    super();
  }


  public RetryException(String message) {
    super(message);
  }


  public RetryException(String message, Throwable cause) {
    super(message, cause);
  }


  public RetryException(Throwable cause) {
    super(cause);
  }
}
