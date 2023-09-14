package com.oppo.cloud.application;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HdfsWriter {

    private static FileSystem fs;
    private static Configuration hdfsconf = new Configuration();

    static {
        System.setProperty("java.security.krb5.conf", "C:\\Users\\22047328\\Desktop\\keberos\\青岛测试\\krb5.conf");
        hdfsconf.set("fs.defaultFS", "hdfs://nameservice1");
        hdfsconf.set("dfs.nameservices", "nameservice1");
        hdfsconf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        hdfsconf.set("dfs.ha.automatic-failover.enabled.nameservice1", "true");
        hdfsconf.set("ha.zookeeper.quorum", "qdedhtest0:2181,qdedhtest1:2181,qdedhtest2:2181");
        hdfsconf.set("dfs.ha.namenodes.nameservice1", "namenode30,namenode172");
        hdfsconf.set("dfs.namenode.rpc-address.nameservice1.namenode30", "qdedhtest0:8020");
        hdfsconf.set("dfs.namenode.rpc-address.nameservice1.namenode172", "qdedhtest1:8020");
        hdfsconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hdfsconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hdfsconf.setBoolean("hadoop.security.authentication", true);
        hdfsconf.set("hadoop.security.authentication", "kerberos");
        hdfsconf.set("dfs.namenode.kerberos.principal.pattern", "hdfs/*@HAIER.COM");
        UserGroupInformation.setConfiguration(hdfsconf);
        try {
            UserGroupInformation.loginUserFromKeytab("admin", "C:\\Users\\22047328\\Desktop\\keberos\\青岛测试\\admin.keytab");
            fs = FileSystem.get(hdfsconf);
            System.out.println("############ hdfs krb 认证通过");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new RuntimeException("get FileSystem fail");
        }
    }

    public static void main(String[] args) throws Exception {
     String logPath = "/flume/airflow/bigdata/bdp_jt/airflow/logs/JOB_SPARK_COMPASS_01/JOB_SPARK_COMP/scheduled__2023-04-17T02_00_00_00_00-1";

        FileStatus[] fileStatuses = fs.globStatus(new Path(String.format("%s*", logPath)));
        for (FileStatus fileStatus : fileStatuses) {
            if (fs.exists(fileStatus.getPath())) {


                System.out.println(fileStatus.getPath().toString());
                FSDataInputStream fsDataInputStream = fs.open(new Path("hdfs://nameservice1:8020/flume/airflow/bigdata/bdp_jt/airflow/logs/JOB_SPARK_COMPASS_01/JOB_SPARK_COMP/scheduled__2023-04-17T02_00_00_00_00-1.log.1681802443241"));

                String[] lines = readLines(fileStatus.getPath().toString());
//                Pattern pattern = Pattern.compile("^.*Submitted application (?<applicationId>application_[0-9]+_[0-9]+).*$");
//                // 提取关键字。TODO appid？
//                for (String line : lines) {
//                    Matcher matcher = pattern.matcher(line);
//                    if (matcher.matches()) {
//                        String matchVal = matcher.group("applicationId");
//
//                        System.out.println(matchVal);
//                    }
//                }


            }
        }


    }

    /**
     * 读取文件，返回日志内容
     */
    public static String[] readLines(String filePath) throws Exception {
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = fs.open(new Path(filePath));
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // 64kb
            byte[] buffer = new byte[65536];
            int byteRead;

            while ((byteRead = fsDataInputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, byteRead);
            }

            byte[] bytes = outputStream.toByteArray();
            String datas = new String(bytes, StandardCharsets.UTF_8);
            return datas.split("\n");
        } catch (Exception e) {
            throw new Exception(String.format("failed to read file: %s, err: %s", filePath, e.getMessage()));
        } finally {
            if (Objects.nonNull(fsDataInputStream)) {
                fsDataInputStream.close();
            }
        }
    }


}
///flume/airflow/bigdata/bdp_jt/airflow/logs/JOB_COMPASS_HIVE_02/COMPASS_HIVE_02/2023-04-14T07_40_00_00_00-1.log.1681458471801
///flume/airflow/bigdata/bdp_jt/airflow/logs/JOB_COMPASS_HIVE_02/COMPASS_HIVE_02/2023-04-14T07_40_00_00_00.1*
