package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util;

import java.util.Arrays;
import java.util.List;

public class Const {

    public static final String REQUEST_URL = "http://check.qd-hongdao.haier.net/sql-check/parse/check";
//    public static final String REQUEST_URL = "http://test-check.qd-hongdao.haier.net/sql-check/parse/check";

    public static final List<String> EXCEL_HEAD = Arrays.asList("脚本ID", "脚本名称", "脚本类型", "SQL", "数据库类型", "脚本创建人", "脚本创建人所属组", "脚本创建人所属组管理员", "SQL评分", "SQL评分明细");

    public static final String GROUP_BY_REGEX = "(\\s+|\\n+)group(\\s+|\\n+)by(\\s+|\\n+)";
    public static final String UNION_REGEX = "(\\s+|\\n+)union(\\s+|\\n+)";
    public static final String JOIN_REGEX = "(\\s+|\\n+)join(\\s+|\\n+)";
    public static final String ORDER_BY_REGEX = "(\\s+|\\n+)order(\\s+|\\n+)by(\\s+|\\n+)";
    public static final String TABLE_NAME_REGEX = "(\\s+|\\n+|.)TABLE_NAME(\\s+|\\n+|$)";
    public static final String INSERT_REGEX = "insert.+table.+partition\\(.+\\)(\\s+|\\n+)";
    public static final String MEMORY_CONF_REGEX = "mapreduce.(map|reduce).memory.mb";

    public static final String SQL_GROUP_BY_NAME = "聚合Group次数";
    public static final int SQL_GROUP_BY_THRESHOLD = 5;
    public static final double SQL_GROUP_BY_SCORE = 2.0;
    public static final String SQL_GROUP_BY_DESC = String.format("一个SQL聚合Group次数最多为%s次，之后每增加一次，分数-%s",
            SQL_GROUP_BY_THRESHOLD, SQL_GROUP_BY_SCORE);

    public static final String SQL_UNION_NAME = "并联Union次数";
    public static final int SQL_UNION_THRESHOLD = 5;
    public static final double SQL_UNION_SCORE = 2.0;
    public static final String SQL_UNION_DESC = String.format("一个SQL并联Union次数最多%s次，之后每增加一次，分数-%s",
            SQL_UNION_THRESHOLD, SQL_UNION_SCORE);

    public static final String SQL_JOIN_NAME = "关联Join次数";
    public static final int SQL_JOIN_THRESHOLD = 5;
    public static final double SQL_JOIN_SCORE = 2.0;
    public static final String SQL_JOIN_DESC = String.format("一个SQL关联Join次数最多为%s次，之后每增加一次，分数-%s",
            SQL_JOIN_THRESHOLD, SQL_JOIN_SCORE);

    public static final String SQL_ORDER_BY_NAME = "排序Order次数";
    public static final int SQL_ORDER_BY_THRESHOLD = 1;
    public static final double SQL_ORDER_BY_SCORE = 2.0;
    public static final String SQL_ORDER_BY_DESC = String.format("一个SQL排序Order次数最多%s次，之后每增加一次，分数-%s",
            SQL_ORDER_BY_THRESHOLD, SQL_ORDER_BY_SCORE);

    public static final String SQL_LENGTH_NAME = "SQL长度";
    public static final int SQL_LENGTH_THRESHOLD = 5000;
    public static final double SQL_LENGTH_SCORE = 0.1;
    public static final String SQL_LENGTH_DESC = String.format("一个SQL长度最多为%s，之后长度每增加1000，分数-%s",
            SQL_LENGTH_THRESHOLD, SQL_LENGTH_SCORE);

    public static final String SQL_TABLE_ERF_NAME = "引用表个数";
    public static final int SQL_TABLE_ERF_THRESHOLD = 5;
    public static final double SQL_TABLE_ERF_SCORE = 2.0;
    public static final String SQL_TABLE_ERF_DESC = String.format("一个SQL内引用表个数最多为%s个，之后每增加一个，分数-%s",
            SQL_TABLE_ERF_THRESHOLD, SQL_TABLE_ERF_SCORE);

    public static final String SQL_TABLE_READ_NAME = "使用表次数";
    public static final int SQL_TABLE_READ_THRESHOLD = 10;
    public static final double SQL_TABLE_READ_SCORE = 0.5;
    public static final String SQL_TABLE_READ_DESC = String.format("一个SQL使用表次数最多为%s次，之后每增加一次，分数-%s",
            SQL_TABLE_READ_THRESHOLD, SQL_TABLE_READ_SCORE);

    public static final String SQL_SCAN_FILE_COUNT_NAME = "文件数量";
    public static final int SQL_SCAN_FILE_COUNT_THRESHOLD = 10;
    public static final double SQL_SCAN_FILE_COUNT_SCORE = 0.5;
    public static final String SQL_SCAN_FILE_COUNT_DESC = String.format("一个SQL扫描文件数量最多为%s个，之后每增加一个,分数-%s",
            SQL_SCAN_FILE_COUNT_THRESHOLD, SQL_SCAN_FILE_COUNT_SCORE);

    public static final String SQL_SCAN_FILE_SIZE_NAME = "文件大小";
    public static final long SQL_SCAN_FILE_SIZE_THRESHOLD = 1024 * 1024 * 1024;
    public static final double SQL_SCAN_FILE_SIZE_SCORE = 0.1;
    public static final String SQL_SCAN_FILE_SIZE_DESC = String.format("一个SQL扫描文件大小最大为%sM，之后每增加100M,分数-%s",
            SQL_SCAN_FILE_SIZE_THRESHOLD / 1024 / 1024, SQL_SCAN_FILE_SIZE_SCORE);


    public static final String SQL_SCAN_SMALL_FILE_COUNT_NAME = "小文件数量";
    public static final int SQL_SCAN_SMALL_FILE_COUNT_THRESHOLD = 10;
    public static final double SQL_SCAN_SMALL_FILE_COUNT_SCORE = 0.2;
    public static final String SQL_SCAN_SMALL_FILE_COUNT_DESC = String.format("一个SQL扫描小文件数量最多为%s个，之后每增加一个，分数-%s",
            SQL_SCAN_SMALL_FILE_COUNT_THRESHOLD, SQL_SCAN_SMALL_FILE_COUNT_SCORE);

    public static final String SQL_SCAN_PARTITION_COUNT_NAME = "分区数量";
    public static final int SQL_SCAN_PARTITION_COUNT_THRESHOLD = 1;
    public static final double SQL_SCAN_PARTITION_COUNT_SCORE = 0.5;
    public static final String SQL_SCAN_PARTITION_COUNT_DESC = String.format("一个SQL扫描分区数量最多为%s个，之后每增加一个，分数-%s",
            SQL_SCAN_PARTITION_COUNT_THRESHOLD, SQL_SCAN_PARTITION_COUNT_SCORE);

}
