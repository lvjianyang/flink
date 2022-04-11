package quick.jdbc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class JdbcSinkExample {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String url = params.get("url","jdbc:mysql://mysql:3306/sql-demo?autoReconnect=true&useSSL=true");
        String driver = params.get("driver","com.mysql.cj.jdbc.Driver");
        String username = params.get("username","sql-demo");
        String password = params.get("password","demo-sql");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Book> list=new ArrayList<>();
        for(int i=0;i<10;i++){
            Book b=new Book(i,"title"+i,"author"+i,i+50);
            list.add(b);
        }

        DataStream<Book> dataStream= env
                .fromElements(list)
                .flatMap(new FlatMapFunction< List<Book>, Book>() {
                    @Override
                    public void flatMap(List<Book> value, Collector<Book> out)
                            throws Exception {
                        for(Book book: value){
                            out.collect(book);
                        }
                    }
                });


        dataStream.addSink(JdbcSink.sink(
                "insert into book (id, title, author, price) values (?,?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.getId());
                    ps.setString(2, t.getTitle());
                    ps.setString(3, t.getAuthor());
                    ps.setInt(4, t.getPrice());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driver)
                        .withUsername(username)
                        .withPassword(password)
                        .build()));

        env.execute("jdbcsink job");
    }


}
