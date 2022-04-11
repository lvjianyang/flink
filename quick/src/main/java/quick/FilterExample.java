package quick;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =     StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> dataStream = env
                .fromElements(0,1, 2, 3, 4, 5)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value != 0;
                    }
                });

        dataStream.print();

        env.execute("FilterExample job");
    }

}
