package quick.table;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableExample {
    public static void main(String[] args) throws Exception {
        String sql="CREATE TABLE source_table (\n" +
                "    user_id INT,\n" +
                "    cost DOUBLE,\n" +
                "    ts AS localtimestamp,\n" +
                "    WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second'='5',\n" +
                "\n" +
                "    'fields.user_id.kind'='random',\n" +
                "    'fields.user_id.min'='1',\n" +
                "    'fields.user_id.max'='10',\n" +
                "\n" +
                "    'fields.cost.kind'='random',\n" +
                "    'fields.cost.min'='1',\n" +
                "    'fields.cost.max'='100'\n" +
                ")\n";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(sql);
        // 执行查询
        Table table = tableEnv.sqlQuery("select * from source_table");

        DataStream<Row> resultStream = tableEnv.toDataStream(table);

        // add a printing sink and execute in DataStream API
        resultStream.print();

        env.execute();
    }
}
