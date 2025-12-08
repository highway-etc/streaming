package com.highway.etc.common;

import java.io.Serializable;

/**
 * Static metadata for each ETC gantry.
 */
public class StationInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    public int stationId;
    public String name;
    public double lat;
    public double lon;

    public boolean hasGeo() {
        return lat != 0.0 || lon != 0.0;
    }
}
