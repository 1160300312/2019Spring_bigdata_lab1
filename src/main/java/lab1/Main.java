package lab1;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Main {
	public static void main(String args[]){
		String inputpath = "hdfs://127.0.0.1:9000/user/Administrator/data/lab1/D_Normalized&Standardized/part-*";
//		String inputpath = "hdfs://127.0.0.1:9000/user/Administrator/data/lab1/large_data.txt";

		String outputpath = "hdfs://127.0.0.1:9000/user/Administrator/data/lab1/D_Preprocessed";
		SparkConf sparkConf = new SparkConf().setAppName("lab1");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");
		JavaRDD<String> input_rdd = sc.textFile(inputpath);
		
		
		Handler handler = new Handler();
		JavaRDD<POI_Review> transformed_rdd = handler.transformer(input_rdd); 
//		JavaRDD<POI_Review> sample_result = handler.sample(transformed_rdd);
//		JavaRDD<POI_Review> filter_result = handler.filter(transformed_rdd);
//		JavaRDD<POI_Review> standandnorm_result = handler.standandnorm(transformed_rdd);
		JavaRDD<POI_Review> preprocess_result = handler.preprocess(transformed_rdd);
//		preprocess_result.collect();
		//preprocess_result.saveAsTextFile(outputpath);
		
	}
}
