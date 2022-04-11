package quick;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByExample  {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, Integer>> dataStream = env
                .fromElements(Tuple2.of(1,1),Tuple2.of(1,2),Tuple2.of(2,2),Tuple2.of(2,2))
                .keyBy(value -> value.f0)
                .sum(1);

        dataStream.print();

        env.execute("KeyByExample job");
    }


}