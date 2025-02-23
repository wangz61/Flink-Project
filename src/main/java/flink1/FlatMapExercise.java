package flink1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapExercise {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = environment.fromElements("white", "blue", "red");
        stream.flatMap(new MyFlatMapper()).print();
        environment.execute();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.equals("white")) {
                out.collect(value);
                out.collect(value);
                out.collect(value);
            } else if (value.equals("blue")) {
                out.collect(value);
                out.collect(value);
            }
        }
    }
}

