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
    @Column(name = "taxi1")
    public Integer taxi1;
    @Column(name = "count1")
    public Integer count1;
    @Column(name = "taxi2")
    public Integer taxi2;
    @Column(name = "count2")
    public Integer count2;
    @Column(name = "taxi3")
    public Integer taxi3;
    @Column(name = "count3")
    public Integer count3;

    public TripDurationStatistics() {
       
    }

    public TripDurationStatistics(float min, float max, float avg, double stddev, Date date, int taxi1, int count1, int taxi2, int count2, int taxi3, int count3)
    {
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.stddev = stddev;
        this.date = date;
        this.taxi1 = taxi1;
        this.count1 = count1;
        this.taxi2 = taxi2;
        this.count2 = count2;
        this.taxi3 = taxi3;
        this.count3 = count3;
    }

    

    @Override
    public String toString() {
        return "min: " + this.min.toString() + "max: " + this.max.toString() 
            + "avg: " + this.avg.toString() + "stddev: " + this.stddev.toString() + "date: " + this.date.toString()
            + "taxi1: " + this.taxi1.toString() + "count1: " + this.count1.toString() + "taxi2: " + this.taxi2.toString() + "count2: " +
            this.count2.toString() + "taxi3: " + this.taxi3.toString() + "count3: " + this.count3.toString(); 
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

    public Integer getTaxi1() {
        return taxi1;
    }

    public void setTaxi1(Integer taxi1) {
        this.taxi1 = taxi1;
    }

    public Integer getCount1() {
        return count1;
    }

    public void setCount1(Integer count1) {
        this.count1 = count1;
    }

    public Integer getTaxi2() {
        return taxi2;
    }

    public void setTaxi2(Integer taxi2) {
        this.taxi2 = taxi2;
    }

    public Integer getCount2() {
        return count2;
    }

    public void setCount2(Integer count2) {
        this.count2 = count2;
    }

    public Integer getTaxi3() {
        return taxi3;
    }

    public void setTaxi3(Integer taxi3) {
        this.taxi3 = taxi3;
    }

    public Integer getCount3() {
        return count3;
    }

    public void setCount3(Integer count3) {
        this.count3 = count3;
    }

    
}
