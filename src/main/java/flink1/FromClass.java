package flink1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromClass {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> stream = environment.fromElements(
                new Person("Tom", 30),
                new Person("Jerry", 32),
                new Person("Bob", 2)
        );
        DataStream<Person> adults = stream.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person value) throws Exception {
                return value.age <= 15;
            }
        });
        adults.print();
        environment.execute();
    }
    public static class Person {
        public String name;
        public Integer age;
        public Person() {

        }
        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
        public String toString() {
            return this.name + " age : " + this.age.toString();
        }
    }
}