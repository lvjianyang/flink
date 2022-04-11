package quick;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceExample  {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, Integer>> dataStream = env
                .fromElements(Tuple2.of(1,1),Tuple2.of(1,2),Tuple2.of(2,2),Tuple2.of(2,2))
                .keyBy(value -> value.f0)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2)
                            throws Exception {
                        return new Tuple2(value1.f0 , value1.f1+value2.f1);
                    }
                });

        dataStream.print();

        env.execute("ReduceExample job");


    }


}
