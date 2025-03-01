package flink3;

import flink1.ClickSource;
import flink1.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessingTimeTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = environment.addSource(new ClickSource());
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value,
                                               KeyedProcessFunction<String, Event, String>.Context ctx,
                                               Collector<String> out) throws Exception {
                        long curr = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + " Data Processing Time: " + new Timestamp(curr));
                        ctx.timerService().registerProcessingTimeTimer(curr + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "Triggered: " + new Timestamp(timestamp));
                    }

                }).print();
        environment.execute();
    }
}
