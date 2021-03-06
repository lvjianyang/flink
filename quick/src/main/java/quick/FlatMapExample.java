package quick;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env
                .fromElements("Flink Spark Storm","Flink Flink Flink")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out)
                            throws Exception {
                        for(String word: value.split(" ")){
                            out.collect(word);
                        }
                    }
                });

        dataStream.print();

        env.execute("FlatMapExample job");

    }
}
