package flink4;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class FrequentUserAnalysis {

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

        stream.keyBy(data -> data.userId)
                .process(new FrequentUserDetetction())
                .print();
        environment.execute();
    }

    public static class FrequentUserDetetction extends KeyedProcessFunction<String, UserBehavior, String> {

        private transient ValueState<Long> visitCount;
        private transient ValueState<Long> timerState;

        public void open(Configuration parameter) throws Exception {
            visitCount = getRuntimeContext().getState(new ValueStateDescriptor<>("visitCount", Long.class, 0L));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", Long.class));
        }

        @Override
        public void processElement(UserBehavior value, KeyedProcessFunction<String, UserBehavior, String>.Context ctx, Collector<String> out) throws Exception {
            long count = visitCount.value() + 1;
            visitCount.update(count);

            if (timerState.value() == null) {
                long midnight = (ctx.timestamp() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000);
                ctx.timerService().registerEventTimeTimer(midnight);
                timerState.update(midnight);
            }
        }

        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long count = visitCount.value();
            if (count > 3) {
                out.collect("Frequent User: " + ctx.getCurrentKey() + " Visit Count: " + count);
            }
            visitCount.clear();
            timerState.clear();
        }
    }

}
