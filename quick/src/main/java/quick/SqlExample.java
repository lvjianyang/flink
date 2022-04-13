package quick;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;


public class SqlExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE book (" +
                "  id INT," +
                "  title STRING," +
                "  author STRING," +
                "  price INT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://mysql:3306/sql-demo'," +
                "   'table-name' = 'book'," +
                "   'driver'='com.mysql.jdbc.Driver'," +
                "   'username' = 'sql-demo'," +
                "   'password' = 'demo-sql'" +
                ")");

            // execute SELECT statement
        TableResult tableResult1 = tableEnv.executeSql("SELECT * FROM book");
        // use try-with-resources statement to make sure the iterator will be closed automatically
        try (CloseableIterator<Row> it = tableResult1.collect()) {
            while(it.hasNext()) {
                Row row = it.next();
                // handle row
                System.out.println("row: " + row.toString());
            }
        }
        // execute Table
        TableResult tableResult2 = tableEnv.sqlQuery("SELECT * FROM book").execute();
        System.out.println(tableResult2.getJobClient().get().getJobStatus());
        tableResult2.print();
        env.execute("SqlExample");
    }
}
