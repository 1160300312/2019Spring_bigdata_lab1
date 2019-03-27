package lab1;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class Test {
	public static void main(String args[]){
	/*	String path = "hdfs://127.0.0.1:9000/input/README.txt";
		String outputpath = "hdfs://127.0.0.1:9000/out/result.txt";
		SparkConf sparkConf = new SparkConf().setAppName("Java-WordCount");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String>  a = sc.textFile(path, 1);
		JavaRDD<String> words = a.flatMap(new FlatMapFunction<String,String>(){

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String arg0) throws Exception {
				return	Arrays.asList(arg0.split(" ")).iterator();
			}
		});
		words.saveAsTextFile(outputpath);
		List<String> list = words.collect();
		for(int i=0;i<list.size();i++){
			System.out.println(list.get(i));
		}*/
		//System.out.println(Double.parseDouble("1.02"));
//		String word = "1|2".replaceAll("\\|", "i");
//		System.out.println(word);
		Tuple2<String,String> a = new Tuple2<String,String>("a","b");
		Tuple2<String,String> b = new Tuple2<String,String>("a","b");
		System.out.println(a.equals(b));
		
	}
}
