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

package com.oppo.cloud.portal.service.diagnose.runtime;

import com.alibaba.fastjson2.JSONObject;
import com.oppo.cloud.common.constant.AppCategoryEnum;
import com.oppo.cloud.common.domain.eventlog.DetectorResult;
import com.oppo.cloud.common.domain.eventlog.SqlScoreAbnormal;
import com.oppo.cloud.common.domain.eventlog.config.DetectorConfig;
import com.oppo.cloud.portal.domain.diagnose.runtime.SqlScore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * sql 评分过低
 */
@Service
public class SqlScoreService extends RunTimeBaseService<SqlScore> {

    @Override
    public String getCategory() {
        return AppCategoryEnum.SQL_SCORE_ANOMALY.getCategory();
    }

    @Override
    public SqlScore generateData(DetectorResult detectorResult, DetectorConfig config) throws Exception {
        SqlScoreAbnormal sqlScoreAbnormal = ((JSONObject) detectorResult.getData()).toJavaObject(SqlScoreAbnormal.class);

        SqlScore sqlScore = new SqlScore();
        sqlScore.setAbnormal(sqlScoreAbnormal.getAbnormal() != null && sqlScoreAbnormal.getAbnormal());

        List<SqlScore.TaskInfo> data = sqlScore.getTable().getData();
        SqlScore.TaskInfo taskInfo = new SqlScore.TaskInfo();
        taskInfo.setSqlScore(sqlScoreAbnormal.getScore());
        taskInfo.setMinScore(config.getSqlScoreConfig().getMinScore());
        taskInfo.setSqlScoreContent(sqlScoreAbnormal.getDiagnoseResult());
        data.add(taskInfo);

        sqlScore.getVars().put("sqlScore", String.valueOf(sqlScoreAbnormal.getScore()));
        sqlScore.getVars().put("minScore", String.valueOf(config.getSqlScoreConfig().getMinScore()));

        return sqlScore;
    }

    @Override
    public String generateConclusionDesc(Map<String, String> thresholdMap) {
        return String.format("SQL评分不能低于%s", thresholdMap.getOrDefault("minScore", "60"));
    }

    @Override
    public String generateItemDesc() {
        return "SQL评分过低";
    }

    @Override
    public String getType() {
        return "table";
    }
}