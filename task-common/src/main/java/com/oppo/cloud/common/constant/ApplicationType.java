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

package com.oppo.cloud.common.constant;


import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum ApplicationType {

    SPARK("SPARK"),
    MAPREDUCE("MAPREDUCE"),
    FLINK("Apache Flink"),
    DATAX("DATAX");

    private final String value;
    ApplicationType(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static ApplicationType getInstance(String strAppType) throws IllegalArgumentException{
        for (ApplicationType applicationType : ApplicationType.values()){
            if(applicationType.value.equals(strAppType)){
                return applicationType;
            }
        }
        throw new IllegalArgumentException("invalid app type : " + strAppType);
    }

    private static final Map<String, ApplicationType> MAP;

    static {
        Map<String, ApplicationType> map = new ConcurrentHashMap<>();
        for (ApplicationType instance : ApplicationType.values()) {
            map.put(instance.getValue(), instance);
        }
        MAP = Collections.unmodifiableMap(map);
    }

    public static ApplicationType get(String name) {
        return MAP.get(name);
    }

    @Override
    public String toString() {
        return value;
    }
}
