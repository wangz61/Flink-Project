package flink2;

import flink1.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EventProcessing {

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
                new Event("Tom", "/home", 8000L)
        );
        DataStream<String> mappedstream = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user + " visited " + event.url;
            }
        });
        DataStream<Event> filteredstream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Tom");
            }
        });
        mappedstream.print("MappedFunction output: ");
        filteredstream.print("FilteredFunction output: ");
        stream.filter(data -> data.user.equals("Bob")).print("Lambda Filter :");
        environment.execute();
    }
    public static class MyMapper implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.user + " visited " + event.url;
        }
    }
}
