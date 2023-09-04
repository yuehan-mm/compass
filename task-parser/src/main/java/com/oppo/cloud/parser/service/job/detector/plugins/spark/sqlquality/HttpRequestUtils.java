package com.oppo.cloud.parser.service.job.detector.plugins.spark.sqlquality;

import com.google.common.collect.Maps;
import okhttp3.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 封装请求
 *
 * @author weiximei
 */
public class HttpRequestUtils {


    /**
     * JSON数据格式请求
     *
     * @param url
     * @param header
     * @param json
     * @return
     */
    private static String json(String url, Map<String, Object> header, String json) throws IOException {
        // 创建一个请求 Builder
        Request.Builder builder = new Request.Builder();
        // 创建一个 request
        Request request = builder.url(url).build();

        // 创建一个 Headers.Builder
        Headers.Builder headerBuilder = request.headers().newBuilder();

        // 装载请求头参数
        Iterator<Map.Entry<String, Object>> headerIterator = header.entrySet().iterator();
        headerIterator.forEachRemaining(e -> {
            headerBuilder.add(e.getKey(), (String) e.getValue());
        });

        // application/octet-stream
        RequestBody requestBody = FormBody.create(json, MediaType.parse("application/json"));

        // 设置自定义的 builder
        builder.headers(headerBuilder.build()).post(requestBody);

        OkHttpClient client = new OkHttpClient()
                .newBuilder()
                .connectTimeout(120, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .writeTimeout(120, TimeUnit.SECONDS)
                .build();


        try (Response execute = client.newCall(builder.build()).execute()) {
            return execute.body().string();
        }
    }


    /**
     * post请求  参数JSON格式
     *
     * @param url
     * @param json JSON数据
     * @return
     * @throws IOException
     */
    public static String doPost(String url, String json) throws IOException {
        return json(url, Maps.newHashMap(), json);
    }
}
