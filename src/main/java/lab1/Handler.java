package lab1;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Serializable;
import scala.Tuple2;

public class Handler implements Serializable{
	private static final long serialVersionUID = 1L;


	public JavaRDD<POI_Review> transformer(JavaRDD<String> rdd){
		JavaRDD<POI_Review> result = rdd.flatMap(new FlatMapFunction<String,POI_Review>(){

			static final long serialVersionUID = 1L;

			public Iterator<POI_Review> call(String arg0) throws Exception {
				
				
				String line[] = arg0.split("\n");
//				System.out.println(line[0]);
				ArrayList<POI_Review> list = new ArrayList<POI_Review>();
				for(int i=0;i<line.length;i++){
					String[] word = line[i].split("\\|");
					
					POI_Review poi = new POI_Review(word[0],word[1],word[2],
							word[3], word[4], word[5],word[6],
							word[7],word[8],word[9],word[10],word[11]);
					list.add(poi);
					
				}
				return list.iterator();
			}
			
		});
		return result;
	}
	
	
	public JavaRDD<POI_Review> sample(JavaRDD<POI_Review> rdd){
		JavaPairRDD<String,POI_Review> POI_carrer_pairs = rdd.mapToPair(new PairFunction<POI_Review,String,POI_Review>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<String, POI_Review> call(POI_Review arg0) throws Exception {
				return new Tuple2<String, POI_Review>(arg0.user_career,arg0);
			}
			
		});
		
		JavaPairRDD<String, Iterable<POI_Review>> group_result = POI_carrer_pairs.groupByKey();
		JavaRDD<POI_Review> sample_result = group_result.flatMap(
				new FlatMapFunction<Tuple2<String,Iterable<POI_Review>>,POI_Review>(){
					private static final long serialVersionUID = 1L;

					public Iterator<POI_Review> call(Tuple2<String, Iterable<POI_Review>> arg0) throws Exception {
						Random r = new Random();
						List<POI_Review> list = new ArrayList<POI_Review>();
						Iterator<POI_Review> itr = arg0._2.iterator();
						while(itr.hasNext()){
							POI_Review value = itr.next();
							int prob = r.nextInt(100);
							if(prob == 0){
								list.add(value);
							}
						}
						return list.iterator();
					}
				
				});
		return sample_result;
	}
	
	public JavaRDD<POI_Review> filter(JavaRDD<POI_Review> rdd){
		List<POI_Review> list_mid = rdd.collect();
		ArrayList<POI_Review> list = new ArrayList<POI_Review>(list_mid);
		Iterator<POI_Review> itr = list.iterator();
		while(itr.hasNext()){
			POI_Review flag = itr.next();
			if(flag.rating.equals("?")){
				itr.remove();
			}
		}
		Collections.sort(list);
		final POI_Review lower = list.get(list.size()/100);
		final POI_Review upper = list.get(list.size()/100*99);
		JavaRDD<POI_Review> result = rdd.filter(
				new Function<POI_Review ,Boolean>(){
					private static final long serialVersionUID = 1L;
					public Boolean call(POI_Review arg0) throws Exception {
						System.out.println(arg0);
						if(arg0.rating.equals("?")){
							return true;
						}
						if(Double.parseDouble(arg0.rating)>Double.parseDouble(upper.rating) ||
								Double.parseDouble(arg0.rating)<Double.parseDouble(lower.rating) ||
								Double.parseDouble(arg0.longitude)<8.1461259 ||
								Double.parseDouble(arg0.longitude)>11.1993265 ||
								Double.parseDouble(arg0.latitude)<56.5824856 ||
								Double.parseDouble(arg0.latitude)>57.750511){
							return false;
						}
						return true;
					}
					
				});
		return result;
	}

	public JavaRDD<POI_Review> standandnorm(JavaRDD<POI_Review> rdd){
		List<POI_Review> list_mid = rdd.collect();
		ArrayList<POI_Review> list = new ArrayList<POI_Review>(list_mid);
		Iterator<POI_Review> itr = list.iterator();
		while(itr.hasNext()){
			POI_Review flag = itr.next();
			if(flag.rating.equals("?")){
				itr.remove();
			}
		}
		Collections.sort(list);
		final POI_Review lower = list.get(0);
		final POI_Review upper = list.get(list.size()-1);
		String[] months = {"January","February","March","April","May","June","July","August",
				"September", "October","November", "December"};
		final List<String> months_list = Arrays.asList(months);
		JavaRDD<POI_Review> result = rdd.map(new Function<POI_Review, POI_Review>(){
			private static final long serialVersionUID = 1L;

			public POI_Review call(POI_Review arg0) throws Exception {
				if(arg0.review_data.matches("[0-9]*/[0-9]*/[0-9]*")){
					String[] words = arg0.review_data.split("/");
					arg0.review_data = words[0]+"-"+words[1]+"-"+words[2];
				}
				if(arg0.review_data.matches("[A-Za-z]* [0-9]*,[0-9]*")){
					String[] words1 = arg0.review_data.split(" ");
					String[] words2 = words1[1].split(",");
//					System.out.println(words1[0]);
					String month = null;
					if((months_list.indexOf(words1[0])+1)<=9){
						month = "0" + (months_list.indexOf(words1[0])+1);
					} else{
						month = "" + months_list.indexOf(words1[0]);
					}
					arg0.review_data = words2[1]+"-"+ month + "-" + 
					((Integer.parseInt(words2[0])<10)?"0"+words2[0]:words2[0]);
				}
				if(arg0.user_birthday.matches("[0-9]*/[0-9]*/[0-9]*")){
					String[] words = arg0.user_birthday.split("/");
					arg0.user_birthday = words[0]+"-"+words[1]+"-"+words[2];
				}
				if(arg0.user_birthday.matches("[A-Za-z]* [0-9]*,[0-9]*")){
					String[] words1 = arg0.user_birthday.split(" ");
					String[] words2 = words1[1].split(",");
					System.out.println(words1[0]);
					String month = null;
					if((months_list.indexOf(words1[0])+1)<=9){
						month = "0" + (months_list.indexOf(words1[0])+1);
					} else{
						month = "" + months_list.indexOf(words1[0]);
					}
					arg0.user_birthday = words2[1]+"-"+ month + "-" + 
					((Integer.parseInt(words2[0])<10)?"0"+words2[0]:words2[0]);
				}
				if(arg0.temperature.matches("(-?\\d+)(\\.\\d+)?℉")){
					DecimalFormat df = new DecimalFormat("#.0");
					String[] tem = arg0.temperature.split("℉");
					double tem_double = Double.parseDouble(tem[0]);
					double result_tem = (tem_double-32)/1.8;
					if(result_tem>-1&&result_tem<1){
						arg0.temperature = "0" + df.format(result_tem) + "℃";
					} else{
						arg0.temperature = df.format(result_tem) + "℃";
					}
				}
				if(!arg0.rating.equals("?")){
					double rating = (Double.parseDouble(arg0.rating) - Double.parseDouble(lower.rating)) /
							(Double.parseDouble(upper.rating) - Double.parseDouble(lower.rating));
					DecimalFormat df1 = new DecimalFormat("#.00");
					arg0.rating = "0" + df1.format(rating);
				}
//				System.out.println(arg0);
				return arg0;
			}
			
		});
		return result;
	}
	
	public JavaRDD<POI_Review> merge(JavaRDD<POI_Review> rdd){
		JavaRDD<POI_Review> sample_result = sample(rdd);
		
		return null;
	}
	
	public JavaRDD<POI_Review> preprocess(JavaRDD<POI_Review> rdd){
		JavaPairRDD<Tuple2<String,String>,POI_Review> ic_pairs = rdd.mapToPair(
				new PairFunction<POI_Review,Tuple2<String,String>,POI_Review>(){
					private static final long serialVersionUID = 1L;

					public Tuple2<Tuple2<String, String>, POI_Review> call(POI_Review arg0) throws Exception {
						return new Tuple2<Tuple2<String,String>,POI_Review>(new Tuple2<String,String>(arg0.user_nationality,arg0.user_career),arg0);
					}
				});
		JavaPairRDD<Tuple2<String,String>,Iterable<POI_Review>> ic_group = ic_pairs.groupByKey();
		JavaRDD<POI_Review> result = ic_group.flatMap(
				new FlatMapFunction<Tuple2<Tuple2<String,String>,Iterable<POI_Review>>,POI_Review>(){
					private static final long serialVersionUID = 1L;

					public Iterator<POI_Review> call(Tuple2<Tuple2<String, String>, Iterable<POI_Review>> arg0)
							throws Exception {
						Iterator<POI_Review> itr1 = arg0._2.iterator();
						Iterator<POI_Review> itr2 = arg0._2.iterator();
						List<POI_Review> list = new ArrayList<POI_Review>();
						double sum = 0;
						int count = 0;
						while(itr1.hasNext()){
							POI_Review mid = itr1.next();
							if(!mid.user_income.equals("?")){
								sum += Double.parseDouble(mid.user_income);
								count += 1;
								list.add(mid);
							}
						}
						double income = sum / count;
						while(itr2.hasNext()){
							POI_Review mid = itr2.next();
							if(mid.user_income.equals("?")){
								mid.user_income = "" + (int)income;
								list.add(mid);
							}
						}
						return list.iterator();
					}
				});
		
		return result;
	}
}
