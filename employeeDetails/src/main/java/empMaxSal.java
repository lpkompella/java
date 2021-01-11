import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
public class empMaxSal {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("EmployeeMaxSalary").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile("src/main/resources/employee_noheader.csv");
        JavaRDD<String> sortedData = rdd.sortBy(new Function<String, Double>() {
            @Override
            public Double call(String arg0) throws Exception {
                String[] data = arg0.split(",");
                return data[2].length() > 0 ? Double.parseDouble(data[2]) : 0.0;
            }
        }, false, 1);
        Iterator<String> top = sortedData.take(1).iterator();
          while (top.hasNext()) {
            System.out.println(top.next());
        }
    }
}