package quick.es;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.List;



public class ElasticsearchFlink {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<User> source = env.addSource(new StreamParallelSource());

        DataStream<User> filterSource = source.filter(new FilterFunction<User>() {
            @Override
            public boolean filter(User s) throws Exception {
                return !s.getName().contains("Jim");
            }
        });

        DataStream<JSONObject> transSource = filterSource.map(customer -> {
            String jsonString = JSONObject.toJSONString(customer, SerializerFeature.WriteDateUseDateFormat);
            System.out.println("当前正在处理:" + jsonString);
            JSONObject jsonObject = JSONObject.parseObject(jsonString);
            return jsonObject;
        });



        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));

        ElasticsearchSink.Builder<JSONObject> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<JSONObject>() {
                    public IndexRequest createIndexRequest(JSONObject json) {
                        return Requests.indexRequest()
                                .index("my-index")
                                .source(json);
                    }

                    @Override
                    public void process(JSONObject user, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(user));
                    }
                }
        );

        // 设置批量写数据的缓冲区大小
        esSinkBuilder.setBulkFlushMaxActions(10);
        transSource.addSink(esSinkBuilder.build());
        env.execute("ElasticsearchFlink job");


    }


}
