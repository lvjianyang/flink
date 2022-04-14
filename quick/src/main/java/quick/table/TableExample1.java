package quick.table;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class TableExample1 {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //数据源
        Table table = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "ABC"),
                row(2L, "ABCDE")
        );
        //过滤
        Table result = table.where($("name").isEqual("ABCDE"));
        // print

        DataStream<Row> resultStream = tEnv.toDataStream(result);

        // add a printing sink and execute in DataStream API
        resultStream.print();

        env.execute();
    }
}
