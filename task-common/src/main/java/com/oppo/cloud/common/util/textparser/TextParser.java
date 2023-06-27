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

package com.oppo.cloud.common.util.textparser;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * 文本解析
 */
@Slf4j
public class TextParser implements ITextParser {

    /**
     * 所有的action规则
     */
    private List<ParserAction> actions;
    /**
     * 单个文本匹配到的action规则
     */
    private final Map<String, ParserAction> matchActions;
    /**
     * 正在匹配的action，只支持单个action
     */
    private ParserAction parsingAction;
    /**
     * 匹配到的内容
     */
    private List<String> blocks;
    /**
     * 模板匹配位置
     */
    private PositionState state;

    public TextParser(ParserAction action) {
        this.state = PositionState.HEAD;
        List<ParserAction> newActions = new ArrayList<>();
        newActions.add(action);
        this.actions = newActions;
        matchActions = new HashMap<>();
        blocks = new ArrayList<>();
    }

    public TextParser(List<ParserAction> actions) {
        this.state = PositionState.HEAD;
        if (actions != null) {
            actions.sort(Comparator.comparing(ParserAction::getStep));
            this.actions = actions;
        }
        matchActions = new HashMap<>();
        blocks = new ArrayList<>();
    }

    /**
     * 解析
     */
    @Override
    public void parse(String line) {
        switch (this.state) {
            case HEAD:
                matchHeadsTemplate(line);
                break;
            case MIDDLE:
                matchMiddleTemplate(line);
                break;
            case TAIL:
                matchTailsTemplate(line);
                break;
            default:
                break;
        }
    }

    /**
     * 获取解析结果
     */
    @Override
    public Map<String, ParserAction> getResults() {
        return this.matchActions;
    }

    /**
     * 头部匹配
     *
     * @param line
     */
    private void matchHeadsTemplate(String line) {
        if (this.actions != null) {
            for (ParserAction parserAction : this.actions) {
                ParserTemplate parserTemplate = parserAction.getParserTemplate();
                if (parserAction.isSkip() || parserTemplate == null || parserTemplate.getHeads() == null ||
                        parserTemplate.getHeads().size() == 0) {
                    continue;
                }
                List<Pattern> patterns = parserTemplate.getHeads();
                for (Pattern pattern : patterns) {
                    Matcher m = pattern.matcher(line);
                    if (m.matches()) {
                        this.blocks.add(line);
                        this.parsingAction = parserAction;
                        List<Pattern> middlePatterns = parserTemplate.getMiddles();
                        List<Pattern> tailPatterns = parserTemplate.getTails();
                        if (middlePatterns != null && middlePatterns.size() > 0) {
                            extractGroupData(m);
                            this.state = PositionState.MIDDLE;
                        } else if (tailPatterns != null && tailPatterns.size() > 0) {
                            extractGroupData(m);
                            this.state = PositionState.TAIL;
                        } else {
                            // 只有head,相当于行匹配
                            setParserResults(m);
                        }
                        return;
                    }
                }
            }
        }
    }

    /**
     * 中间部分匹配
     *
     * @param line
     */
    private void matchMiddleTemplate(String line) {
        this.blocks.add(line);
        if (this.parsingAction != null) {
            List<Pattern> patterns = this.parsingAction.getParserTemplate().getMiddles();
            boolean isMatches = false;
            for (Pattern pattern : patterns) {
                Matcher m = pattern.matcher(line);
                if (m.matches()) {
                    // 中间部分兼容多行多正则提取
                    extractGroupData(m);
                    isMatches = true;
                    // return;
                }
            }
            if (isMatches) {
                this.state = PositionState.TAIL;
            }
        }
    }

    /**
     * 尾部匹配
     *
     * @param line
     */
    private void matchTailsTemplate(String line) {
        this.blocks.add(line);
        if (this.parsingAction != null) {
            List<Pattern> patterns = this.parsingAction.getParserTemplate().getTails();
            for (Pattern pattern : patterns) {
                Matcher m = pattern.matcher(line);
                if (m.matches()) {
                    // 不包含尾部行
                    this.blocks.remove(line);
                    setParserResults(m);
                    return;
                }
            }
        }
    }

    /**
     * 保存匹配结果
     */
    private void setParserResults(Matcher m) {
        List<ParserResult> parserResults = this.parsingAction.getParserResults();
        if (parserResults == null) {
            parserResults = new ArrayList<>();
        }
        ParserResult parserResult = new ParserResult();
        parserResult.setLines(this.blocks);
        extractGroupData(m);
        Map<String, String> groupData = this.parsingAction.getGroupData();
        parserResult.setGroupData(groupData == null ? null : new HashMap(groupData));
        int hashCode = this.blocks.toString().hashCode();
        Set<Integer> hashCodeSet = this.parsingAction.getHashCode();

        if (hashCodeSet != null && hashCodeSet.contains(hashCode)) {
            log.debug("parserAction skip contain log:{}", this.blocks);
        } else {
            if (hashCodeSet == null) {
                hashCodeSet = new HashSet<>();
                hashCodeSet.add(hashCode);
            }
            this.parsingAction.setHashCode(hashCodeSet);

            parserResults.add(parserResult);
            this.parsingAction.setParserResults(parserResults);
            this.parsingAction.setMatchSucceed(true);
            this.matchActions.put(this.parsingAction.getAction(), this.parsingAction);
        }

        this.blocks = new ArrayList<>();
        this.parsingAction = null;
        this.state = PositionState.HEAD;
    }

    private void extractGroupData(Matcher m) {
        // 尾行当标志位，不参与提取
        if (this.parsingAction.getGroupNames() != null && this.state != PositionState.TAIL) {
            Map<String, String> groupMap = this.parsingAction.getGroupData();
            if (groupMap == null) {
                groupMap = new HashMap<>();
            }
            for (String name : this.parsingAction.getGroupNames()) {
                try {
                    String v = m.group(name);
                    groupMap.put(name, v);
                    this.parsingAction.setGroupData(groupMap);
                } catch (Exception e) {
                    if (StringUtils.isBlank(groupMap.get(name))) {
                        groupMap.put(name, "");
                    }
                }
            }
        }
    }

}
