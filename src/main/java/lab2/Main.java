package lab2;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {
	public static void main(String args[]){
//		String inputpath = "hdfs://127.0.0.1:9000/user/Administrator/data/lab1/D_Normalized&Standardized/part-*";
		String inputpath1 = "hdfs://127.0.0.1:9000/user/Administrator/data/lab2/train";
		String inputpath2 = "hdfs://127.0.0.1:9000/user/Administrator/data/lab2/test";

		String outputpath = "hdfs://127.0.0.1:9000/user/Administrator/data/lab1/D_Preprocessed";
		SparkConf sparkConf = new SparkConf().setAppName("lab2").setMaster("local").set("spark.driver.host", "localhost").set("spark.driver.maxResultSize", "4g");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");
		//JavaRDD<String> input_rdd = sc.textFile(inputpath);
		JavaRDD<String> input_rdd_train = sc.textFile(inputpath1);
		JavaRDD<String> input_rdd_test = sc.textFile(inputpath2);
		
		
		Handler h = new Handler();
		
		JavaPairRDD<Integer, float[] > input_train = h.handleClassifyFile(input_rdd_train);
		JavaPairRDD<Integer, float[] > input_test = h.handleClassifyFile(input_rdd_test);
		
		//h.NativeBayes(h.handleClassifyFile(input_rdd_train), h.handleClassifyFile(input_rdd_test));
		
		h.trainLogistic(input_train, (float)0.1, (float)0.01, input_test);
		
		/*Handler h = new Handler();
		JavaRDD<int[]> mid = h.handleFile(input_rdd);
		List<int[]> kmeans = h.Kmeans(mid, 8);
		List<int[]> data = mid.collect();
		for(int i=0;i<10000;i++){
			int min = h.countDistance(kmeans.get(0), data.get(i));
			int index = 0;
			for(int j=1;j<kmeans.size();j++){
				if(min > h.countDistance(kmeans.get(j), data.get(i))){
					min = h.countDistance(kmeans.get(j), data.get(i));
					index = j;
				}
			}
			System.out.println(index);
		}*/
		
	}
}
