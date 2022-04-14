package quick.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;



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
        Table resultTable = tableEnv.sqlQuery("SELECT * FROM book");

        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        // 打印
        resultStream.print();
        env.execute();
    }
}
