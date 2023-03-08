package com.flink;

import java.util.Date;
import java.util.HashMap;

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

        HashMap<Integer, Integer> taxis = new HashMap<>();

        for (InputMessage msg : elements) {
            count ++;
            sum += msg.trip_duration;
            if (msg.trip_duration > max)
                max = msg.trip_duration;
            if (msg.trip_duration < min)
                min = msg.trip_duration;
            if(!taxis.containsKey(msg.taxi_id)) {
                taxis.put(msg.taxi_id, 1);
            }
            else {
                int val = taxis.get(msg.taxi_id) + 1;
                taxis.replace(msg.taxi_id, val);
            }
        } 

        int taxi1 = (int)taxis.keySet().toArray()[0];
        int count1 = taxis.get(taxi1);
        int taxi2 = (int)taxis.keySet().toArray()[1];
        int count2 = taxis.get(taxi2);
        int taxi3 = (int)taxis.keySet().toArray()[2];
        int count3 = taxis.get(taxi3);

        avg = sum / count;
        for (InputMessage msg : elements) {
            stddev += Math.pow(msg.trip_duration - avg, 2);
        }

        stddev = Math.sqrt(stddev / count);
        Date date = new Date();

        TripDurationStatistics res = new TripDurationStatistics(min, max, avg, stddev, date, taxi1, count1, taxi2, count2, taxi3, count3);
        out.collect(res);
    }

}