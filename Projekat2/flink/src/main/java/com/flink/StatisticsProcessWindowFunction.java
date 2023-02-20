package com.flink;

import java.util.Date;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StatisticsProcessWindowFunction extends ProcessAllWindowFunction<InputMessage, TripDurationStatistics, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<InputMessage, TripDurationStatistics, TimeWindow>.Context context,
            Iterable<InputMessage> elements, Collector  <TripDurationStatistics> out) throws Exception {
        float sum = 0;
        float max = 0;
        float min = 50000;
        float avg = 0;
        double stddev = 0;
        float count = 0;

        for (InputMessage msg : elements) {
            count ++;
            sum += msg.trip_duration;
            if (msg.trip_duration > max)
                max = msg.trip_duration;
            if (msg.trip_duration < min)
                min = msg.trip_duration;
            
        } 

        avg = sum / count;
        for (InputMessage msg : elements) {
            stddev += Math.pow(msg.trip_duration - avg, 2);
        }

        stddev = Math.sqrt(stddev / count);
        Date date = new Date();

        TripDurationStatistics res = new TripDurationStatistics(min, max, avg, stddev, date);
        System.out.println("final res ---> " + res);
        out.collect(res);
        
    }

}