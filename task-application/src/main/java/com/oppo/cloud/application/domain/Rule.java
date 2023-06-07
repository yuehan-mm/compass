/*
 * Copyright 2023 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.cloud.application.domain;

import lombok.Data;

import java.util.regex.Pattern;

/****
 * AirFlow 日志解析规则
 * 从脚本执行日志中解析作业的类型与作业执行信息
 */
@Data
public class Rule {
    private String type;    // 作业类型
    private String regex;   // 作业匹配正则表达式
    private String name;    // 提取关键字

    private Pattern pattern = null;


    synchronized public Pattern getPattern(){
        if(pattern == null){
            pattern = Pattern.compile(regex);
        }
        return pattern;
    }
}
