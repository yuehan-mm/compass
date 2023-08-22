package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;

public class Const {

    public static final String REQUEST_URL = "http://check.qd-hongdao.haier.net/sql-check/parse/check";
//    public static final String REQUEST_URL = "http://test-check.qd-hongdao.haier.net/sql-check/parse/check";

    public static final String GROUP_BY_REGEX = "(\\s+|\\n+)group(\\s+|\\n+)by(\\s+|\\n+)";
    public static final String UNION_REGEX = "(\\s+|\\n+)union(\\s+|\\n+)";
    public static final String JOIN_REGEX = "(\\s+|\\n+)join(\\s+|\\n+)";
    public static final String ORDER_BY_REGEX = "(\\s+|\\n+)order(\\s+|\\n+)by(\\s+|\\n+)";
    public static final String TABLE_NAME_REGEX = "(\\s+|\\n+)TABLE_NAME(\\s+|\\n+)";
    public static final String INSERT_REGEX = "insert.+table.+partition\\(.+\\)(\\s+|\\n+)";
    public static final String MEMORY_CONF_REGEX = "mapreduce.(map|reduce).memory.mb";

    public static final int SQL_GROUP_BY_THRESHOLD = 3;
    public static final int SQL_GROUP_BY_SCORE = 5;
    public static final String SQL_GROUP_BY_DESC = "一个SQL内最多进行3次分组，之后每增加一次，分数-5";

    public static final int SQL_UNION_THRESHOLD = 2;
    public static final int SQL_UNION_SCORE = 5;
    public static final String SQL_UNION_DESC = "同一个SQL内最多进行2次union，之后每增加一次，分数-5";


    public static final int SQL_JOIN_THRESHOLD = 4;
    public static final int SQL_JOIN_SCORE = 10;
    public static final String SQL_JOIN_DESC = "一个SQL内join次数最多为4次，之后每增加一次，分数-10";


    public static final int SQL_ORDER_BY_THRESHOLD = 1;
    public static final int SQL_ORDER_BY_SCORE = 10;
    public static final String SQL_ORDER_BY_DESC = "一个SQL内最多进行1次排序，之后每增加一次，分数-10";


    public static final int SQL_LENGTH_THRESHOLD = 1000;
    public static final int SQL_LENGTH_SCORE = 1;
    public static final String SQL_LENGTH_DESC = "SQL长度阈值为1000，之后长度每增加1000，分数-1";


    public static final int SQL_READ_TABLE_THRESHOLD = 5;
    public static final int SQL_READ_TABLE_SCORE = 5;
    public static final String SQL_READ_TABLE_DESC = "读取表的个数阈值为5，之后长度每增加1，分数-5";


    public static final int SQL_TABLE_USE_THRESHOLD = 2;
    public static final int SQL_TABLE_USE_SCORE = 5;
    public static final String SQL_TABLE_USE_DESC = "同一个SQL内相同表最多可使用2次，之后每使用一次，分数-5";

}
