import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by admin on 2018/6/13.
 */
public class SparkKNN {


    static JavaSparkContext createSc(String name){
        SparkSession ss = SparkSession.builder()
                .master("local")
                .appName(name)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
        return JavaSparkContext.fromSparkContext(ss.sparkContext());

    }

    /**
     * 按照测试数据集的唯一标示进行聚合,
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter ft = DateTimeFormatter.ofPattern("hh:mm:ss");

        for (String arg: args){
            System.out.println(arg);
        }

        if(args.length < 4){
            System.err.println("args not enough");
            System.exit(1);
        }

        String hdfs = "hdfs://localhost:9000/ch13";
        String local = "file:///Users/admin/Desktop";

        Integer k = Integer.parseInt(args[0]);
        Integer d = Integer.parseInt(args[1]);

        String dr_fp = hdfs + "/" + args[2]; // 文件路径
        String ds_fp = hdfs + "/" + args[3];

        JavaSparkContext jsc =  SparkKNN.createSc("knn_demo");
        // 设置广播变量:有哪些值需要广播: k和d
        final Broadcast<Integer> bk = jsc.broadcast(k);
        final Broadcast<Integer> bd = jsc.broadcast(d);

        JavaRDD<String> r = jsc.textFile(dr_fp); // test data
        r.saveAsTextFile(local + "/R/" + now.format(ft));

        JavaRDD<String> s = jsc.textFile(ds_fp); // trained data
        s.saveAsTextFile(local + "/S/" + now.format(ft));

        // 创建笛卡尔积
        JavaPairRDD<String, String> cart = r.cartesian(s);
        cart.saveAsTextFile(local+"/output/cart" + now.format(ft));

        // 提取 pairrdd , key为r数据集的唯一表示, v为 tuple 元组, 元组key为距离, value为分类号
        JavaPairRDD<String, Tuple2<Double, String>> jpr = cart.mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<Double, String>>() {
            public Tuple2<String, Tuple2<Double, String>> call(Tuple2<String, String> cart_meta) throws Exception {

                String rRecord = cart_meta._1;
                String sRecord = cart_meta._2;

                String[] rTokens = rRecord.split(";");
                String[] sTokens = sRecord.split(";");

                String r_id = rTokens[0];
                String r_properties = rTokens[1];

                String s_classifer_id = sTokens[1];
                String s_properties = sTokens[2];

                Integer d = bd.value();

                double distance = Util.cacuDis(r_properties, s_properties, d);
                Tuple2<Double, String> v = new Tuple2<Double, String>(distance, s_classifer_id);
                return new Tuple2<String, Tuple2<Double, String>>(r_id, v);
            }
        });

        //----------------------------------
        ///**
        jpr.saveAsTextFile(local + "/output/knnmaped" + now.format(ft));
        // 到当前步骤我们获取到了包含所有r和s中的组合的pairrdd对象,其元组key为 r唯一标识,value为一个元组, key为 r到s点距离, value为 s所属分类
        // 接下来的目标是聚合
        JavaPairRDD<String, Iterable<Tuple2<Double, String>>> knnGrouped = jpr.groupByKey();

        JavaPairRDD<String, String> result = knnGrouped.mapValues(new Function<Iterable<Tuple2<Double, String>>, String>() {
            public String call(Iterable<Tuple2<Double, String>> knn_meta) throws Exception {
                SortedMap<Double, String> sorted_map = Util.findNereasetK(knn_meta, bk.value());
                Map<String, Integer> count_map = Util.classCount(sorted_map);
                return Util.determineClass(count_map);
            }
        });
        result.saveAsTextFile(local + "/output/knnoutput" + now.format(ft));
        //*/
        //----------------------------------
        // 以下是使用 combineByKey版本实现相同功能
        /**
        Function<Tuple2<Double, String>, String> createCombiner = new Function<Tuple2<Double, String>, String>() {
            public String call(Tuple2<Double, String> tuple2s) throws Exception {
                String str = "";
                str = str + tuple2s._1 + "," + tuple2s._2 + "|";
                return str;
            }
        };

        Function2<String, Tuple2<Double, String>, String> mergeValue = new Function2<String, Tuple2<Double, String>, String>() {
            public String call(String s, Tuple2<Double, String> tuple2s) throws Exception {

                s = s + tuple2s._1 + "," + tuple2s._2 + "|";
                return s;
            }
        };

        Function2<String, String, String> mergeCombiners = new Function2<String, String, String>() {
            public String call(String s, String s2) throws Exception {
                Integer k = bk.value();
                s = s + s2;
                List<Tuple2<Double, String>> al = new ArrayList<Tuple2<Double, String>>();

                String[] meta1 = s.split("|");


                for (String meta2: meta1){

                    if (meta2 != ""){

                        String[] tmp = meta2.split(",");
                        al.add(new Tuple2<Double, String>(Double.parseDouble(tmp[0]), tmp[1]));
                    }
                }
                SortedMap<Double, String> collect_k = Util.findNereasetK(al, k);
                Map<String, Integer> classifier_count = Util.classCount(collect_k);
                return Util.determineClass(classifier_count);
            }
        };

        JavaPairRDD<String, String> result2 = jpr.combineByKey(createCombiner, mergeValue, mergeCombiners);
        result2.saveAsTextFile(local + "/output/knnoutput_2" + now.format(ft));
        */
    }


}
