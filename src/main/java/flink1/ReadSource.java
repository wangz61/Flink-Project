package flink1;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class ReadSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream1 = environment.fromElements("white", "blue", "red");
        DataStream<Integer> dataStream2 = environment.fromElements(1, 2, 3, 4, 5);
        DataStream<String> dataStream3 = environment.fromCollection(Arrays.asList("white", "blue", "red"));
        DataStream<Long> dataStream4 = environment.generateSequence(10, 20);

        dataStream4.print();
        environment.execute();
    }
}
