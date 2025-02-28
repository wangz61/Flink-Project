package flink2;

import flink1.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformPartitionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        DataStreamSource<Event> stream = environment.fromElements(
                new Event("Mary", "/home", 1000L),
                new Event("Tom", "/cart", 2000L),
                new Event("Bob", "/fav", 3000L),
                new Event("Mary", "/fav", 4000L),
                new Event("Bob", "/home", 5000L),
                new Event("Mary", "/cart", 6000L),
                new Event("Bob", "/cart", 7000L),
                new Event("Tom", "/home", 8000L)
        );
        //stream.shuffle().print().setParallelism(4);
        stream.rebalance().print().setParallelism(4);
        environment.execute();
    }
}
