package com.highway.etc.common;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * 窗口统计结果模型，供窗口聚合与 MySqlStatsSink 使用。
 */
public class StatsRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    public int stationId;
    public Instant windowStart;
    public Instant windowEnd;
    public long count;
    public Map<String, Long> byDir = Collections.emptyMap();
    public Map<String, Long> byType = Collections.emptyMap();
}
