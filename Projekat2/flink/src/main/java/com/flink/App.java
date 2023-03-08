package com.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.mapping.Mapper;


public class App 
{
    public static void main( String[] args ) throws Exception
    {
        final DeserializationSchema<InputMessage> schema = new DeserializationSchemaInputMessage();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<InputMessage> source = KafkaSource.<InputMessage>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("taxiporto")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(schema))
            .build();

        DataStream<InputMessage> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").filter(new FilterFunction<InputMessage>() {
            @Override
            public boolean filter(InputMessage value) throws Exception {
                return (value.start_time > 1372636800 && value.start_time < 1375315199);
            }
        });
        DataStream<TripDurationStatistics> res = ds.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).process(new StatisticsProcessWindowFunction());

        CassandraSink.addSink(res)
            .setMapperOptions(() -> new Mapper.Option[] {
                Mapper.Option.saveNullFields(true)
            })
            .setClusterBuilder(new ClusterBuilder() {
                private static final long serialVersionUID = 1L;

                @Override
                protected Cluster buildCluster(Builder builder) {

                    return builder.addContactPoints("cassandra-node").withPort(9042).build();
                }
            })
            .build();
        env.execute("Taxi");

    }

}

