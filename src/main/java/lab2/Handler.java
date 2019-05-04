package lab2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Serializable;
import scala.Tuple2;

public class Handler implements Serializable{
	private static final long serialVersionUID = 1L;

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
	
	public int[] countSum(int[] a, int[] b){
		for(int i=0;i<a.length;i++){
			a[i] += b[i];
		}
		return a;
	}
	
	public List<int[]> Kmeans(JavaRDD<int[]> input, int k){
		List<int[]> collect_result = input.collect();
		Random r = new Random();
		final List<int[]> mid = new ArrayList<int[]>();
		for(int i=0;i<k;i++){
			mid.add(collect_result.get(r.nextInt(collect_result.size())));
		}
		//maptopair key为簇的标号
		
		
		while(true){
			System.out.println(1);
			List<int[]> judger = new ArrayList<int[]>();
			for(int i=0;i<mid.size();i++){
				judger.add(mid.get(i));
			}
			this.displayArray(judger);
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
			
//			E_result.collect();
			
			
			//reducebykey
			JavaPairRDD<Integer, Iterable<int[]>> group_result = E_result.groupByKey();
			
//			group_result.collect();
			
			//M step
			
			
			JavaRDD<int[]> M_result = group_result.map(new Function<Tuple2<Integer,Iterable<int[]>>,int[]>(){
				private static final long serialVersionUID = 1L;

				public int[] call(Tuple2<Integer, Iterable<int[]>> arg0) throws Exception {
					Handler h = new Handler();
					Iterator<int[]> it = arg0._2.iterator();
					int[] next = it.next();
					int [] avg = new int[next.length];
					h.countSum(avg, next);
					int size = 1;
					while(it.hasNext()){
						int[] n = it.next();
						h.countSum(avg, n);
						size ++;
					}
					for(int i=0;i<avg.length;i++){
						avg[i] /= size;
					}
 					return avg;
				}
				
			});
			
			List<int[]> M_collect = M_result.collect();
			mid.clear();
			mid.addAll(M_collect);
			
			
//			M_result.collect();
			this.displayArray(mid);
			if(this.judgeKmeansStop(mid, judger)){
				return mid;
			}
		}
	}
	
	public boolean judgeKmeansStop(List<int[]> a, List<int[]> b){
		for(int i=0;i<a.size();i++){
			int flag = 0;
			for(int j=0;j<b.size();j++){
				if(this.countDistance(a.get(i), b.get(j)) == 0){
					flag = 1;
				}
			}
			if(flag != 1){
				return false;
			}
		}
		return true;
	}
	
	public void displayArray(List<int[]> b){
		if(b.size() == 0){
			System.out.println("no element!");
		}
		for(int k=0;k<b.size();k++){
			int [] a = b.get(k);
			for(int i=0;i<a.length-1;i++){
				System.out.print(a[i] + " ");
			}
			System.out.println(a[a.length-1]);
		}
	}
	
	public void writeResult(String input,String output){
		Configuration conf = new Configuration();
		Path p = new Path(input);
		try {
			FileSystem fs = FileSystem.get(conf);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));
			String line;
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	public JavaPairRDD<Integer, float[]> handleClassifyFile(JavaRDD<String> input){
		JavaPairRDD<Integer,float[]> result = input.mapToPair(new PairFunction<String,Integer,float[]>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, float[]> call(String arg) throws Exception {
				int cls = Integer.parseInt(arg.split("\t")[0]);
				String arg0 = arg.split("\t")[1];
				String[] words = arg0.split(",");
				float[] result = new float[words.length];
				for(int i=0;i<words.length;i++){
					result[i] = Float.parseFloat(words[i]);
				}
				return new Tuple2<Integer,float[]>(cls,result);
			}
		});
		return result;
	}
	
	public void NativeBayes(JavaPairRDD<Integer, float[]> data, JavaPairRDD<Integer, float[]> testdata){
		JavaPairRDD<Integer,Iterable<float[]>> group_data = data.groupByKey();
		JavaPairRDD<Tuple2<Integer,Integer>,Float> mid = group_data.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Iterable<float[]>>,Tuple2<Integer,Integer>,Float>(){
			
			private static final long serialVersionUID = 1L;

			public Iterator<Tuple2<Tuple2<Integer, Integer>, Float>> call(Tuple2<Integer, Iterable<float[]>> arg0)
					throws Exception {
				List<Tuple2<Tuple2<Integer, Integer>, Float>> ret = new ArrayList<Tuple2<Tuple2<Integer, Integer>, Float>>();
				Iterator<float[]> itr = arg0._2.iterator();
				while(itr.hasNext()){
					float[] temp = itr.next();
					for(int i=0;i<temp.length;i++){
						
						ret.add(new Tuple2<Tuple2<Integer, Integer>, Float>(new Tuple2<Integer,Integer>(arg0._1,i),temp[i]));
					}
				}
				return ret.iterator();
			}
		});
		
		JavaPairRDD<Tuple2<Integer,Integer>,Iterable<Float>> mid_group = mid.groupByKey();
		
		JavaPairRDD<Tuple2<Integer,Integer>,Tuple2<Float,Float>> train_result = mid_group.mapToPair(new PairFunction<Tuple2<Tuple2<Integer,Integer>,Iterable<Float>>,Tuple2<Integer,Integer>,Tuple2<Float,Float>>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<Tuple2<Integer, Integer>, Tuple2<Float, Float>> call(
					Tuple2<Tuple2<Integer, Integer>, Iterable<Float>> arg0) throws Exception {
				Handler h = new Handler();
				Iterator<Float> it1 = arg0._2.iterator();
				Iterator<Float> it2 = arg0._2.iterator();
				float avg = h.countAvg(it1);
				float mean = h.countMean(it2, avg);
				return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Float, Float>>(arg0._1,new Tuple2<Float,Float>(avg,mean));
			}
		});
		
		Map<Integer, Float> prioir = new HashMap<Integer,Float>();
		prioir.put(0, (float)0.5);
		prioir.put(1, (float)0.5);
		JavaRDD<Tuple2<Integer,Integer>> rdd_result = this.testNativeBayes(testdata, train_result, prioir);
		List<Tuple2<Integer,Integer>> result = rdd_result.collect();
		int sum = 0;
		int right_sum = 0;
		for(int i=0;i<result.size();i++){
			sum ++;
			if(result.get(i)._1 == result.get(i)._2){
				right_sum++;
			}
		}
		System.out.println((float)(right_sum) / sum);
	}
	
	public Map<Integer, Float> getPriori(JavaPairRDD<Integer,Iterable<float[]>> data){
		Map<Integer, Float> result = new HashMap<Integer, Float>();
		JavaPairRDD<Integer,Integer> mid = data.mapToPair(new PairFunction<Tuple2<Integer,Iterable<float[]>>,Integer,Integer>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, Integer> call(Tuple2<Integer, Iterable<float[]>> arg0) throws Exception {
				Iterator<float[]> it = arg0._2.iterator();
				int count = 0;
				while(it.hasNext()){
					count++;
				}
				System.out.println(count);
				return new Tuple2<Integer,Integer>(arg0._1, count);
			}
		});
		List<Tuple2<Integer,Integer>> mid_num = mid.collect();
		float sum = 0;
		for(int i=0;i<mid_num.size();i++){
			sum += mid_num.get(i)._2;
		}
		for(int i=0;i<mid_num.size();i++){
			result.put(mid_num.get(i)._1, mid_num.get(i)._2/sum);
		}
		return result;
	}
	
	public JavaRDD<Tuple2<Integer,Integer>> testNativeBayes(JavaPairRDD<Integer, float[]> data, JavaPairRDD<Tuple2<Integer,Integer>,Tuple2<Float,Float>> train, final Map<Integer, Float> p){
		List<Tuple2<Tuple2<Integer,Integer>,Tuple2<Float,Float>>> train_result = train.collect();
		final Map<Tuple2<Integer,Integer>,Tuple2<Float,Float>> m = new HashMap<Tuple2<Integer,Integer>,Tuple2<Float,Float>>();
		for(int j=0;j<train_result.size();j++){
			m.put(train_result.get(j)._1, train_result.get(j)._2);
		}
		final Handler h = new Handler();
		JavaRDD<Tuple2<Integer,Integer>> result = data.map(new Function<Tuple2<Integer,float[]>,Tuple2<Integer,Integer>>(){
			private static final long serialVersionUID = 1L;
			public Tuple2<Integer, Integer> call(Tuple2<Integer, float[]> arg0) throws Exception {
				float[] d = arg0._2;
				float sum1 = 1;
				float sum2 = 1;
				for(int i=0;i<d.length;i++){
					Tuple2<Float,Float> tuple = m.get(new Tuple2<Integer,Integer>(0,i));
					sum1 *= h.countGauss(tuple._1, tuple._2, d[i]);
				}
				for(int i=0;i<d.length;i++){
					Tuple2<Float,Float> tuple = m.get(new Tuple2<Integer,Integer>(1,i));
					sum2 *= h.countGauss(tuple._1, tuple._2, d[i]);
				}
				if(p.get(0)*sum1>p.get(1)*sum2){
					return new Tuple2<Integer,Integer>(arg0._1,0);
				} else{
					return new Tuple2<Integer,Integer>(arg0._1,1);
				}
			}
			
		});
		return result;
	}
	
	public float countAvg(Iterator<Float> it){
		float sum = 0;
		int count = 0;
		while(it.hasNext()){
			sum += it.next();
			count ++;
		}
		return sum/count;
	}
	
	public float countMean(Iterator<Float> it, float avg){
		float sum = 0;
		int count = 0;
		while(it.hasNext()){
			float temp = it.next();
			sum += (temp-avg) * (temp-avg);
			count ++;
		}
		return sum/count;
	}
	
	public float countGauss(float avg, float mean, float x){
		return (float) (1.0/Math.sqrt(mean*2*Math.PI)*Math.pow(Math.E, -1*((x-avg)*(x-avg))/(2*mean)));
	}
	
	public float[] trainLogistic(JavaPairRDD<Integer, float[]> input, float alpha, float lambda,JavaPairRDD<Integer, float[]> inputtest){
		final float[] w = new float[19];
		for(int i=0;i<w.length;i++){
			w[i] = 0;
		}
		int cycle = 100;
		int count = 0;
		while(count<cycle){
			JavaRDD<float[]> gradient = input.map(new Function<Tuple2<Integer,float[]>, float[]>(){
				private static final long serialVersionUID = 1L;

				public float[] call(Tuple2<Integer, float[]> arg0) throws Exception {
					Handler h = new Handler();
					float[] result = new float[arg0._2.length+1];
					result[0] = arg0._1 - h.countLogistic(w, arg0._2);
					for(int i=1;i<=arg0._2.length;i++){
						result[i] = arg0._2[i-1] * (arg0._1 - h.countLogistic(w, arg0._2));
					}
					return result;
				}
			});
			List<float[]> collect_result = gradient.collect();
			float[] sum = new float[19];
			for(int i=0;i<sum.length;i++){
				sum[i] = 0;
			}
			for(int i=0;i<collect_result.size();i++){
				for(int j=0;j<19;j++){
					sum[j] += collect_result.get(i)[j];
				}
			}
			for(int i=0;i<19;i++){
				sum[i] /= collect_result.size();
				w[i] = w[i] - alpha * lambda * w[i] - alpha * sum[i];
			}
			count ++;
			for(int i=0;i<w.length-1;i++){
				System.out.print(w[i] + " ");
			}
			System.out.println(w[w.length-1]);;
			System.out.println(this.testLogistic(w, inputtest));
		}
		return w;
	}
	
	public float testLogistic(final float[] w, JavaPairRDD<Integer, float[]> input){
		JavaPairRDD<Integer, Integer> test = input.mapToPair(new PairFunction<Tuple2<Integer,float[]>,Integer,Integer>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<Integer, Integer> call(Tuple2<Integer, float[]> arg0) throws Exception {
				float judge = 0;
				judge += w[0];
				for(int i=0;i<arg0._2.length;i++){
					judge += w[i+1] * arg0._2[i];
				}
				if(judge > 0){
					return new Tuple2<Integer,Integer>(arg0._1, 0);
				} else{
					return new Tuple2<Integer,Integer>(arg0._1, 1);
				}
			}
		});
		
		List<Tuple2<Integer,Integer>> result = test.collect();
		float sum = result.size();
		float right_sum = 0;
		for(int i=0;i<result.size();i++){
			if(result.get(i)._1 == result.get(i)._2){
				right_sum ++;
			}
		}
		System.out.println(right_sum);
		return right_sum / sum;
	}
	
	public float countLogistic(float[] w, float[] x){
		float sum = 0;
		sum += w[0];
		for(int i=0;i<x.length;i++){
			sum += w[i+1] * x[i];
		}
		return (float) (1.0/(1 + Math.exp(sum)));
	}
}
