package quick.es;


import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.security.SecureRandom;
import java.util.Random;

/**
 * 自定义的流式并行数据源
 */
public class StreamParallelSource implements ParallelSourceFunction<User> {

    private boolean isRunning = true;
    private String[] names = new String[5];
    private Random random = new SecureRandom();
    private Long id = 1L;

    public  void init() {
        names[0] = "Tom";
        names[1] = "Jim";
        names[2] = "Ket";
        names[3] = "Cheery";
        names[4] = "Lily";
    }

    /**
     * 每隔10ms生成一个Customer数据对象（模拟获取实时数据）
     */
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        init();
        while(isRunning) {
            int nameIndex = random.nextInt(5);

            User customer = new User(""+id++,names[nameIndex]);
            sourceContext.collect(customer);
            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
