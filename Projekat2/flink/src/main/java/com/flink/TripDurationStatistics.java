package com.flink;

import java.util.Date;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "bigdata", name = "tripduration")
public class TripDurationStatistics {
    @Column(name = "max")
    public Float max;
    @Column(name = "min")
    public Float min;
    @Column(name = "avg")
    public Float avg;
    @Column(name = "stddev")
    public Double stddev;
    @Column(name = "date")
    public Date date;

    public TripDurationStatistics() {
       
    }

    public TripDurationStatistics(float min, float max, float avg, double stddev, Date date)
    {
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.stddev = stddev;
        this.date = date;
    }

    

    @Override
    public String toString() {
        return "min: " + this.min.toString() + "max: " + this.max.toString() 
            + "avg: " + this.avg.toString() + "stddev: " + this.stddev.toString() + "date: " + this.date.toString(); 
    }

    public Float getMax() {
        return max;
    }

    public void setMax(Float max) {
        this.max = max;
    }

    public Float getMin() {
        return min;
    }

    public void setMin(Float min) {
        this.min = min;
    }

    public Float getAvg() {
        return avg;
    }

    public void setAvg(Float avg) {
        this.avg = avg;
    }

    public Double getStddev() {
        return stddev;
    }

    public void setStddev(Double stddev) {
        this.stddev = stddev;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
