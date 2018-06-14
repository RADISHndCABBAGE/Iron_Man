import com.google.common.base.Splitter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * Created by admin on 2018/6/13.
 */



public class Util {

    /**
     * 生成JavaSparkContext
     * @return
     * @throws Exception
     */
    /**
    static JavaSparkContext createSC(String name) throws Exception{
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("name", name);
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }
    /

    /**
     * @param str: 被切割double组成的字符串
     * @param delimiter: 切割符号
     * @return List(double)
     * @throws Exception
     */
    static List<Double> splitToList(String str, String delimiter) throws Exception{

        // 定义一个 splitter
        Splitter sp = Splitter.on(delimiter).trimResults();
        Iterable<String> ite = sp.split(str);
        if(ite != null){
            List<Double> l = new ArrayList<Double>();
            for(String token:ite){
                double data = Double.parseDouble(token);
                l.add(data);
            }
            return l;
        }else{
            return null;
        }
    }

    /**
     * 计算两个点的距离
     * @param r 册数数据集
     * @param s 已标注数据集
     * @param d 维数
     * @return
     * @throws Exception
     */
    static double cacuDis(String r, String s, int d) throws Exception{
        List<Double> lr = splitToList(r, ",");
        List<Double> ls = splitToList(r, ",");

        if(lr.size() != d || ls.size() != d){
            return Double.NaN;
        }

        double sum = 0;
        for (int i=0; i<d; i++){
            double meta = lr.get(i) - ls.get(i);
            sum += meta * meta;
        }
        return Math.sqrt(sum);
    }

    /**
     * 寻找一系列点中的最大的点.
     * @param ite
     * @param k
     * @return
     * @throws Exception
     */
    static SortedMap<Double, String> findNereasetK(Iterable<Tuple2<Double, String>> ite, int k) throws Exception{
        TreeMap<Double, String> tree = new TreeMap<Double, String>();
        for (Tuple2<Double, String> tup: ite){
            Double distance = tup._1;
            String id_str = tup._2;

            // 保留k个 点
            tree.put(distance, id_str);
            if (tree.size() > k){
                tree.remove(tree.lastKey()); // 移除最大的点
            }
        }
        return tree;
    }

    /**
     * k个临近点进行归类
     * @param tree :类别与距离的集合
     * @return
     * @throws Exception
     */
    static Map<String, Integer> classCount(SortedMap<Double, String> tree) throws Exception{
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Map.Entry<Double, String> entry: tree.entrySet()){
            String id_str = entry.getValue();
            Integer count = map.get(id_str);
            if (count == null){
                map.put(id_str, 1);
            }else{
                map.put(id_str, count+1);
            }
        }
        return map;
    }

    /**
     * 判定某个点的类别
     * @param map
     * @return
     * @throws Exception
     */
    static String determineClass(Map<String, Integer> map) throws Exception{
        int vote = 0;
        String determine = "";
        for (Map.Entry<String, Integer> entry: map.entrySet()){
            if (determine == ""){
                vote = entry.getValue();
                determine = entry.getKey();
            }else if(entry.getValue() > vote){
                determine = entry.getKey();
                vote = entry.getValue();
            }
        }
        return determine;
    }



}
