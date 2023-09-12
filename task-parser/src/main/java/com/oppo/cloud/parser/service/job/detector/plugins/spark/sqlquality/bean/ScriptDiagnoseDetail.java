package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ScriptDiagnoseDetail {
    private String script_id;
    private String script_name;
    private String script_type;
    private String command;
    private String db_type;

    private String create_user_id;
    private String create_user_name;

    private String create_user_group_id;
    private String create_user_group_nane;

    private String create_user_group_admin_id;
    private String create_user_group_admin_name;

    private String create_user_root_group_id;
    private String create_user_root_group_name;

    public double score;
    public String diagnoseResult;

    private String data_date;


    private double sqlGroupByDeductScore;
    private int sqlGroupByValue;

    private double sqlUnionDeductScore;
    private int sqlUnionValue;

    private double sqlJoinDeductScore;
    private int sqlJoinValue;

    private double sqlOrderByDeductScore;
    private int sqlOrderByValue;

    private double sqlLengthDeductScore;
    private int sqlLengthValue;

    private double sqlTableRefDeductScore;
    private int sqlTableRefValue;

    private double sqlTableReadDeductScore;
    private int sqlTableReadValue;

    private double sqlScanFileCountDeductScore;
    private int sqlScanFileCountValue;

    private double sqlScanFileSizeDeductScore;
    private long sqlScanFileSizeValue;

    private double sqlScanSmallFileCountDeductScore;
    private int sqlScanSmallFileCountValue;

    private double sqlScanPartitionCountDeductScore;
    private int sqlScanPartitionCountValue;

}
