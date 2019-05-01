package lab2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Handler {
	public JavaRDD<int[]> handleFile(JavaRDD<String> input){
		JavaRDD<int[]> result = input.map(new Function<String, int[]>(){
			private static final long serialVersionUID = 1L;

			public int[] call(String arg0) throws Exception {
				String[] words = arg0.split(",");
				int[] data = new int[words.length-1];
				for(int i=1;i<words.length;i++){
					data[i-1] = Integer.parseInt(words[i]);
				}
				return data;
			}
		});
		return result;
	}
	
	public int countDistance(int[] a, int[] b){
		int result = 0;
		for(int i=1;i<a.length;i++){
			result += (a[i] - b[i]) * (a[i] - b[i]);
		}
		return result;
	}
	
	public JavaRDD<int[]> Kmeans(JavaRDD<int[]> input, int k){
		List<int[]> collect_result = input.collect();
		Random r = new Random();
		final List<int[]> mid = new ArrayList<int[]>();
		for(int i=0;i<k;i++){
			mid.add(collect_result.get(r.nextInt(collect_result.size())));
		}
		//maptopair key为簇的标号
		JavaPairRDD<Integer,int[]> E_result = input.mapToPair(new PairFunction<int[],Integer,int[]>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, int[]> call(int[] arg0) throws Exception {
				int key = 0;
				Handler h = new Handler();
				int min = h.countDistance(arg0,mid.get(0));
				for(int i=0;i<mid.size();i++){
					if(min>h.countDistance(arg0, mid.get(i))){
						min = h.countDistance(arg0, mid.get(i));
						key = i;
					}
				}
				return new Tuple2<Integer, int[]>(key,arg0);
			}
			
		});
		//reducebykey
		
		JavaPairRDD<Integer, Iterable<int[]>> group_result = E_result.groupByKey();
		
		//M step
		
		mid.clear();
		
		return null;
	}
	
	
}
