package lab2;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
	public static void main(String args[]){
//		String inputpath = "hdfs://127.0.0.1:9000/user/Administrator/data/lab1/D_Normalized&Standardized/part-*";
		String inputpath = "hdfs://127.0.0.1:9000/user/Administrator/data/lab1/USCensus1990.data.txt";

		String outputpath = "hdfs://127.0.0.1:9000/user/Administrator/data/lab1/D_Preprocessed";
		SparkConf sparkConf = new SparkConf().setAppName("lab2");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");
		JavaRDD<String> input_rdd = sc.textFile(inputpath);

		List<String> test = input_rdd.collect();
		for(int i=0;i<test.size();i++){
			System.out.println(test.get(i));
		}
		
	}
}
