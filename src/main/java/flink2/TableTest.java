package flink2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useAnyPlanner().inStreamingMode().build();
        StreamTableEnvironment table = StreamTableEnvironment.create(environment, settings);
        DataStream dataStream = environment.readTextFile("C:\\Users\\admin\\IdeaProjects\\flink-feb\\src\\main\\resources\\test.txt");
        DataStream<Tuple2<String, Integer>> sum = dataStream.flatMap(new MyFlatMapper2()).keyBy(0).sum(1);
        table.createTemporaryView("test", sum, $("word"), $("count1"));
        Table table1 = table.from("test");
        Table select1 = table1.select($("word"), $("count1")).filter($("count1").isGreater(1));
        Table select2 = table.sqlQuery("select * from test where count1 > 1");
        table.toRetractStream(select1, Row.class).print();
        table.toRetractStream(select2, Row.class).print();
        environment.execute();
    }
    public static class MyFlatMapper2 implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String words[] = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
