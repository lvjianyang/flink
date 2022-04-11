package quick;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import quick.source.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.util.List;

public class WindowAllExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> inStream = env.addSource(new MyRichSourceFunction());
        DataStream<Tuple3<String, String, Integer>> dataStream = inStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //注意：计算变量为f2
                .maxBy(2);
        dataStream.print();
        env.execute("WindowAllExample job");
    }

    /**
     * 模拟数据持续输出
     */
    public static class MyRichSourceFunction extends RichSourceFunction<Tuple3<String, String, Integer>> {
        @Override
        public void run(SourceContext<Tuple3<String, String, Integer>> ctx) throws Exception {
            List<Tuple3<String, String, Integer>> tuple3List = DataSource.getTuple3ToList();
            for (Tuple3 tuple3 : tuple3List){
                ctx.collect(tuple3);
                System.out.println("----"+tuple3);
                //1秒钟输出一个
                Thread.sleep(1 * 1000);
            }
        }
        @Override
        public void cancel() {
            try{
                super.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
