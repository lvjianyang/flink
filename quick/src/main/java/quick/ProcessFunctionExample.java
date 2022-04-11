package quick;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

public class ProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 默认情况下，Flink将使用处理时间。要改变这个，可以设置时间特征:
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 源数据流
        DataStream<Tuple2<String, String>> stream = env
                .fromElements("good good study","day day up","you see see you")
                .flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, String>> collector) throws Exception {
                        for(String word : line.split("\\W+")){
                            collector.collect(new Tuple2<>(word,"1"));
                        }
                    }
                });

        // 因为模拟数据没有时间戳，所以用此方法添加时间戳和水印
        DataStream<Tuple2<String, String>> withTimestampsAndWatermarks =
                stream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, String>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, String> element) {
                        return System.currentTimeMillis();
                    }
                });

        // 在keyed stream上应用该处理函数
        DataStream<Tuple2<String, Long>> result = withTimestampsAndWatermarks
                .keyBy(0)
                .process(new CountWithTimeoutFunction());

        // 输出查看
        result.print();


        env.execute("ProcessFunctionExample");
    }


}
