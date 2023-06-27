package com.oppo.cloud.detect.util;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : 20012523                                                                                *
 * @date : 2023/6/7                                                                                *
 *================================================================================================*/
public class AppNotFoundException extends Exception{
  static final long serialVersionUID = 937124864221090155L;


  public AppNotFoundException() {
    super();
  }


  public AppNotFoundException(String message) {
    super(message);
  }


  public AppNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }


  public AppNotFoundException(Throwable cause) {
    super(cause);
  }
}
