package com.highway.etc.common;

import java.time.Instant;

public class Alert {
    public String hphmMask;
    public int firstStationId;
    public int secondStationId;
    public long timeGapSec;
    public double distanceKm;
    public double speedKmh;
    public double confidence;
    public Instant generatedAt;

    @Override
    public String toString() {
        return "Alert{" +
                "hphmMask='" + hphmMask + '\'' +
                ", firstStationId=" + firstStationId +
                ", secondStationId=" + secondStationId +
                ", timeGapSec=" + timeGapSec +
                ", distanceKm=" + distanceKm +
                ", speedKmh=" + speedKmh +
                ", confidence=" + confidence +
                ", generatedAt=" + generatedAt +
                '}';
    }
}