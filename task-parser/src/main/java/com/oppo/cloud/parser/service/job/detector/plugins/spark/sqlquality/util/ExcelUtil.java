package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util;

import com.alibaba.fastjson.JSONObject;
import com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.bean.ScriptInfo;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import static com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality.util.Const.EXCEL_HEAD;

public class ExcelUtil {

    private static XSSFWorkbook workbook;
    private static XSSFSheet sheet;
    private static XSSFRow row;
    private static XSSFCell cell;
    private static File file;

    //创建sheet页
    public static void setSheet(String sheetName) {
        workbook = new XSSFWorkbook();
        sheet = workbook.createSheet(sheetName);
    }


    //创建表头
    public static void createHead(List<String> headList) {
        //创建表头，也就是第一行
        row = sheet.createRow(0);
        for (int i = 0; i < headList.size(); i++) {
            cell = row.createCell(i);
            cell.setCellValue(headList.get(i));
        }
    }

    //创建表内容
    public static void createContent(List<ScriptInfo> contentList) {
        //创建表内容，从第二行开始
        for (int i = 0; i < contentList.size(); i++) {
            row = sheet.createRow(i + 1);
            ScriptInfo scriptInfo = contentList.get(i);
            row.createCell(0).setCellValue(scriptInfo.getScript_id());
            row.createCell(1).setCellValue(scriptInfo.getScript_name());
            row.createCell(2).setCellValue(scriptInfo.getScript_type());
            row.createCell(3).setCellValue("");
            row.createCell(4).setCellValue(scriptInfo.getDb_type());
            row.createCell(5).setCellValue(scriptInfo.getCreate_user_name() + "(" + scriptInfo.getCreate_user_id() + ")");
            row.createCell(6).setCellValue(scriptInfo.getCreate_user_group_nane() + "(" + scriptInfo.getCreate_user_group_id() + ")");
            row.createCell(7).setCellValue(scriptInfo.getCreate_user_group_admin_name() + "(" + scriptInfo.getCreate_user_group_admin_id() + ")");
            row.createCell(8).setCellValue(JSONObject.toJSONString(scriptInfo.getDiagnoseResult()));
            row.createCell(9).setCellValue(scriptInfo.getScore());
            row.createCell(10).setCellValue(scriptInfo.getScoreContent());
        }
    }

    //写入文件
    public static void writeToFile(String filePath) {
        file = new File(filePath);
        //将文件保存到指定的位置
        try {
            workbook.write(new FileOutputStream(file));
            System.out.println("写入成功");
            workbook.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void writeExcel(List<ScriptInfo> contentList, String filePath) {
        setSheet("WorkSheet");
        createHead(EXCEL_HEAD);
        createContent(contentList);
        writeToFile(filePath);
    }
}