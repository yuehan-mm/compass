package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util;

public class Const {

    public static final String REQUEST_URL = "http://scada-charging-be.qd-hongdao.haier.net/sql/diagnose/sqlQualityChecker";

    public static final String SQL_SCAN_FILE_COUNT_NAME = "文件数量";
    public static final int SQL_SCAN_FILE_COUNT_THRESHOLD = 30;
    public static final double SQL_SCAN_FILE_COUNT_SCORE = 0.5;
    public static final String SQL_SCAN_FILE_COUNT_DESC = String.format("一个SQL扫描文件数量最多为%s个，之后每增加一个,分数-%s",
            SQL_SCAN_FILE_COUNT_THRESHOLD, SQL_SCAN_FILE_COUNT_SCORE);

    public static final String SQL_SCAN_FILE_SIZE_NAME = "文件大小";
    public static final long SQL_SCAN_FILE_SIZE_THRESHOLD = 1024 * 1024 * 1024 * 10l;
    public static final double SQL_SCAN_FILE_SIZE_SCORE = 0.1;
    public static final String SQL_SCAN_FILE_SIZE_DESC = String.format("一个SQL扫描文件大小最大为%sM，之后每增加100M,分数-%s",
            SQL_SCAN_FILE_SIZE_THRESHOLD / 1024 / 1024, SQL_SCAN_FILE_SIZE_SCORE);


    public static final String SQL_SCAN_SMALL_FILE_COUNT_NAME = "小文件数量";
    public static final int SQL_SCAN_SMALL_FILE_COUNT_THRESHOLD = 10;
    public static final double SQL_SCAN_SMALL_FILE_COUNT_SCORE = 0.2;
    public static final String SQL_SCAN_SMALL_FILE_COUNT_DESC = String.format("一个SQL扫描小文件数量最多为%s个，之后每增加一个，分数-%s",
            SQL_SCAN_SMALL_FILE_COUNT_THRESHOLD, SQL_SCAN_SMALL_FILE_COUNT_SCORE);

    public static final String SQL_SCAN_PARTITION_COUNT_NAME = "分区数量";
    public static final int SQL_SCAN_PARTITION_COUNT_THRESHOLD = 20;
    public static final double SQL_SCAN_PARTITION_COUNT_SCORE = 0.5;
    public static final String SQL_SCAN_PARTITION_COUNT_DESC = String.format("一个SQL扫描分区数量最多为%s个，之后每增加一个，分数-%s",
            SQL_SCAN_PARTITION_COUNT_THRESHOLD, SQL_SCAN_PARTITION_COUNT_SCORE);

}
