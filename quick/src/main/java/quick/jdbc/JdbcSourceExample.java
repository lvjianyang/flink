package quick.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcSourceExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFromMySQL()).print();

        env.execute("jdbc source job");
    }
}
