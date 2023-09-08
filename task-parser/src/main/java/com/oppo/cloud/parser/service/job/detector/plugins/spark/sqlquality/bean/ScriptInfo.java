package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean;

import lombok.Data;

@Data
public class ScriptInfo {

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

    public DiagnoseResult diagnoseResult;
    public double score;
    public String scoreContent;

    public ScriptInfo(String script_id, String script_name, String script_type, String command,
                      String db_type, String create_user_id, String create_user_name,
                      String create_user_group_id, String create_user_group_nane,
                      String create_user_group_admin_id, String create_user_group_admin_name,
                      String create_user_root_group_id, String create_user_root_group_name) {
        this.script_id = script_id;
        this.script_name = script_name;
        this.script_type = script_type;
        this.command = command;
        this.db_type = db_type;
        this.create_user_id = create_user_id;
        this.create_user_name = create_user_name;
        this.create_user_group_id = create_user_group_id;
        this.create_user_group_nane = create_user_group_nane;
        this.create_user_group_admin_id = create_user_group_admin_id;
        this.create_user_group_admin_name = create_user_group_admin_name;
        this.create_user_root_group_id = create_user_root_group_id;
        this.create_user_root_group_name = create_user_root_group_name;
    }
}
