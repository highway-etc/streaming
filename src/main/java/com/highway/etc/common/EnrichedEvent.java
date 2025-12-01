package com.highway.etc.common;

import java.time.Instant;
import java.util.Map;

public class EnrichedEvent extends Event {
    public Map<String,Object> tags; // 车型归属地等补充
    public EnrichedEvent copy() {
        EnrichedEvent e = new EnrichedEvent();
        e.gcxh = this.gcxh;
        e.xzqhmc = this.xzqhmc;
        e.adcode = this.adcode;
        e.kkmc = this.kkmc;
        e.stationId = this.stationId;
        e.fxlx = this.fxlx;
        e.gcsj = this.gcsj;
        e.hpzl = this.hpzl;
        e.hphm = this.hphm;
        e.hphmMask = this.hphmMask;
        e.clppxh = this.clppxh;
        e.tags = this.tags;
        return e;
    }
}