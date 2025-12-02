package com.highway.etc.common;

import java.time.Instant;
import java.util.Map;

public class StatsRecord {
    public int stationId;
    public Instant windowStart;
    public Instant windowEnd;
    public long count;
    public Map<String, Long> byDir;   // 按方向统计
    public Map<String, Long> byType;  // 按车型/号牌种类统计
}