package quick.table;



import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;


public class KafkaSqlExample {
    public static void main(String[] args) throws Exception {

        String sinkSql="CREATE TABLE print_table (\n" +
                " account_id BIGINT,\n" +
                " amount BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")";
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "    user_id  BIGINT,\n" +
                "    item_id      BIGINT,\n" +
                "    category_id      BIGINT,\n" +
                "    behavior      STRING,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'input',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        tEnv.executeSql(sinkSql);
        // 执行查询
        Table table = tEnv.sqlQuery("select account_id,amount from transactions");

        table.executeInsert("KafkaSqlExample");
    }
}
