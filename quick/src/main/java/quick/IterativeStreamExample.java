package quick;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class IterativeStreamExample {

    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> someIntegers =env.generateSequence(0, 10);

        // 创建迭代流
        IterativeStream<Long> iteration =someIntegers.iterate();

        // 增加处理逻辑，对元素执行减一操作。
        DataStream<Long> minusOne =iteration.map(new MapFunction<Long, Long>() {

            @Override

            public Long map(Long value) throws Exception {

                return value - 1 ;

            }

        });
        // 获取要进行迭代的流，
        DataStream<Long> stillGreaterThanZero= minusOne.filter(new FilterFunction<Long>() {

            @Override

            public boolean filter(Long value) throws Exception {

                return (value > 0);

            }

        });
        // 对需要迭代的流形成一个闭环
        iteration.closeWith(stillGreaterThanZero);
        // 小于等于0的数据继续向前传输
        DataStream<Long> lessThanZero =minusOne.filter(new FilterFunction<Long>() {

            @Override

            public boolean filter(Long value) throws Exception {

                return (value <= 0);

            }

        });
        minusOne.print();
        env.execute("IterativeStream job");

    }
}
