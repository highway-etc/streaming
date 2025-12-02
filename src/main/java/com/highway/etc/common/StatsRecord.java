package com.highway.etc.common;

import java.time.Instant;
import java.util.Map;

/**
 * 窗口统计结果模型，供窗口聚合与 MySqlStatsSink 使用
 */
public class StatsRecord {
    public int stationId;
    public Instant windowStart;
    public Instant windowEnd;
    public long count;
    public Map<String, Long> byDir;   // 按方向统计
    public Map<String, Long> byType;  // 按号牌种类统计
}