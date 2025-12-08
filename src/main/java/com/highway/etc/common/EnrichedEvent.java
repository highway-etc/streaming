package com.highway.etc.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Event after enrichment has filled in defaults and derived tags.
 */
public class EnrichedEvent extends Event {

    public Map<String, Object> tags = Collections.emptyMap();

    public static EnrichedEvent from(Event src) {
        EnrichedEvent e = new EnrichedEvent();
        e.gcxh = src.gcxh;
        e.xzqhmc = src.xzqhmc;
        e.adcode = src.adcode;
        e.kkmc = src.kkmc;
        e.stationId = src.stationId;
        e.fxlx = src.fxlx;
        e.gcsj = src.gcsj;
        e.hpzl = src.hpzl;
        e.hphm = src.hphm;
        e.hphmMask = src.hphmMask;
        e.clppxh = src.clppxh;
        return e;
    }

    public EnrichedEvent copy() {
        EnrichedEvent c = EnrichedEvent.from(this);
        c.tags = tags == null || tags.isEmpty() ? Collections.emptyMap() : new HashMap<>(tags);
        return c;
    }
}
