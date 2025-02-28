package flink2;

import flink1.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
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
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).max("timestamp").print("Max: ");
        stream.keyBy(data -> data.user).maxBy("timestamp").print("Maxby: ");
        environment.execute();
    }
}
