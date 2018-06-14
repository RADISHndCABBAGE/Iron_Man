import java.util.Map;
import java.util.TreeMap;

/**
 * Created by admin on 2018/6/13.
 */


public class TreeMapDemo {
    public static void main(String[] args) {
        // creating tree map
        TreeMap<Integer, String> treemap = new TreeMap<Integer, String>();

        // populating tree map
        treemap.put(6, "six");
        treemap.put(5, "five");
        treemap.put(2, "two");
        treemap.put(1, "one");
        treemap.put(3, "three");

        // Putting value at key 3
        System.out.println("Value before modification: "+ treemap);
//        System.out.println("Value returned: "+ treemap.put(3,"TP"));
//        System.out.println("Value after modification: "+ treemap);

        for(Map.Entry<Integer, String> entry: treemap.entrySet()){
            if (entry.getKey() < 4){
                System.out.println(entry.getValue());
            }
        }
    }
}
