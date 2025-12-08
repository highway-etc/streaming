package com.highway.etc.common;

import java.io.Serializable;
import java.time.Instant;

/**
 * Raw ETC gantry pass-through event as produced by upstream Kafka. Kept mutable
 * to stay Flink POJO compatible and fast to serialize.
 */
public class Event implements Serializable {

    private static final long serialVersionUID = 1L;

    public long gcxh;
    public String xzqhmc;
    public int adcode;
    public String kkmc;
    public int stationId;
    public String fxlx;
    public Instant gcsj;
    public String hpzl;
    /**
     * Raw plate, only used inside the job.
     */
    public String hphm;
    /**
     * Masked plate for downstream persistence.
     */
    public String hphmMask;
    public String clppxh;

    public Event copy() {
        Event c = new Event();
        c.gcxh = this.gcxh;
        c.xzqhmc = this.xzqhmc;
        c.adcode = this.adcode;
        c.kkmc = this.kkmc;
        c.stationId = this.stationId;
        c.fxlx = this.fxlx;
        c.gcsj = this.gcsj;
        c.hpzl = this.hpzl;
        c.hphm = this.hphm;
        c.hphmMask = this.hphmMask;
        c.clppxh = this.clppxh;
        return c;
    }

    public boolean hasEssentialFields() {
        return stationId > 0 && gcsj != null;
    }
}
