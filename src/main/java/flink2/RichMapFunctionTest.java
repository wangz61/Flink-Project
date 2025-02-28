package flink2;

import flink1.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichMapFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(6);
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Mary", "/home", 1000L),
                new Event("Tom", "/cart", 2000L),
                new Event("Bob", "/fav", 3000L),
                new Event("Mary", "/home", 4000L),
                new Event("Bob", "/home", 5000L),
                new Event("Mary", "/home", 6000L),
                new Event("Bob", "/cart", 7000L),
                new Event("Mary", "/home", 8000L)
        );
        stream.map(new MyRichMapper()).print();
        environment.execute();
    }
    public static class MyRichMapper extends RichMapFunction<Event, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("start of the lifecycle: " + getRuntimeContext().getIndexOfThisSubtask());
        }
        @Override
        public Integer map(Event value) throws Exception {
            return value.user.length();
        }
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("end of the lifecycle: " + getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
