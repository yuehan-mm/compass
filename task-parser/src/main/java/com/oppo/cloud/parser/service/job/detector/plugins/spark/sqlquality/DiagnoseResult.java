package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DiagnoseResult {
    private int groupByCount;
    private int unionCount;
    private int joinCount;
    private int orderByCount;
    private int sqlLength;
    private List<String> insertList;
    private List<String> memConfList;
    private Map<String, Integer> refTableMap;


    public DiagnoseResult(int groupByCount, int unionCount, int joinCount, int orderByCount, List<String> insertList,
                          List<String> memConfList, int sqlLength, Map<String, Integer> refTableMap) {
        this.groupByCount = groupByCount;
        this.unionCount = unionCount;
        this.joinCount = joinCount;
        this.orderByCount = orderByCount;
        this.insertList = insertList;
        this.memConfList = memConfList;
        this.sqlLength = sqlLength;
        this.refTableMap = refTableMap;
    }
}
