package quick.source;

import org.apache.flink.api.java.tuple.Tuple3;
import java.util.Arrays;
import java.util.List;

/**
 * @Description 公共示例数据
 */
public class DataSource {

    /**
     * 示例数据集合
     * Tuple3 是一个固定3个属性变量的实体类，分别用f0,f1,f2表示三个构造传参与变量
     * @return
     */
    public static List<Tuple3<String,String,Integer>> getTuple3ToList(){
        //Tuple3<f0,f1,f2> = Tuple3<姓名，性别（man男，girl女），年龄>
        return Arrays.asList(
                new Tuple3<>("张三", "man", 20),
                new Tuple3<>("李四", "girl", 24),
                new Tuple3<>("王五", "man", 29),
                new Tuple3<>("刘六", "girl", 32),
                new Tuple3<>("伍七", "girl", 18),
                new Tuple3<>("吴八", "man", 30)
        );
    }

}
