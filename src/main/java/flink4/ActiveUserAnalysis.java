package flink4;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class ActiveUserAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<UserBehavior> stream = environment.readTextFile("C:\\Users\\admin\\IdeaProjects\\flink-feb\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event,timestamp) -> event.timestamp));

/*        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new ActiveUserAgg(), new ActiveUserWindowResult("Hourly Active Users"))
                .print();
        environment.execute();*/
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .aggregate(new ActiveUserAgg(), new ActiveUserWindowResult("Active Users Per 10 minutes"))
                .print();
        environment.execute();
    }

    public static class ActiveUserAgg implements AggregateFunction<UserBehavior, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(UserBehavior value, HashSet<String> accumulator) {
            accumulator.add(value.userId);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }

    public static class ActiveUserWindowResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {
        private String label;

        public ActiveUserWindowResult(String label) {
            this.label = label;
        }

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long activeUsers = elements.iterator().next();
            out.collect(label + " Window: " +
                    new Timestamp(context.window().getStart())
                    + "-----" +
                    new Timestamp(context.window().getEnd())
                    + " Active Users: " + activeUsers
            );
        }
    }
}
