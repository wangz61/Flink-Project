package flink3;

import flink1.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Mary", "/home", 1000L),
                new Event("Tom", "/cart", 2000L),
                new Event("Bob", "/fav", 3000L),
                new Event("Mary", "/home", 4000L),
                new Event("Bob", "/home", 5000L),
                new Event("Mary", "/home", 6000L),
                new Event("Bob", "/cart", 7000L),
                new Event("Mary", "/home", 8000L),
                new Event("Mary", "/home", 9000L),
                new Event("Tom", "/cart", 10000L),
                new Event("Bob", "/fav", 11000L),
                new Event("Mary", "/home", 14000L),
                new Event("Bob", "/home", 15000L),
                new Event("Mary", "/home", 16000L),
                new Event("Bob", "/cart", 17000L),
                new Event("Mary", "/home", 18000L)
        );
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        ).map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        }).keyBy(data -> data.f0)
                //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //.countWindow(5, 2)
                //.window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(2)))
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }).print();
        environment.execute();
    }
}
