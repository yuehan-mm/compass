package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util;


import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author Yuanqiang.Zhang
 * @since 2023/4/11
 */
public class MySqlUtils {

    private static String ip = "10.138.225.197";
    private static int port = 3306;
    private static String db = "bdmp_cluster";
    private static String username = "cluster_manager";
    private static String password = "cluster_MANAGER@!#$13";


    /**
     * 获取数据库链接
     *
     * @return Connection
     */
    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&useUnicode=true&characterEncoding=UTF-8", ip, port, db);
            return DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

