package lab1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Handler {
	public JavaRDD<POI_Review> transformer(JavaRDD<String> rdd){
		JavaRDD<POI_Review> result = rdd.flatMap(new FlatMapFunction<String,POI_Review>(){

 static final long serialVersionUID = 1L;

			public Iterator<POI_Review> call(String arg0) throws Exception {
				String line[] = arg0.split("\n");
				ArrayList<POI_Review> list = new ArrayList<POI_Review>();
				for(int i=0;i<line.length;i++){
					String[] word = line[i].split("|");
					POI_Review poi = new POI_Review(word[0],Double.parseDouble(word[1]),Double.parseDouble(word[2]),
							Double.parseDouble(word[3]), word[4], word[5],Double.parseDouble(word[6]),
							word[7],word[8],word[9],word[10],Double.parseDouble(word[11]));
					list.add(poi);
					
				}
				return list.iterator();
			}
			
		});
		return result;
	}
	
	
	public JavaPairRDD<String, List<POI_Review>> sample(JavaRDD<POI_Review> rdd){
		JavaPairRDD<String,POI_Review> POI_carrer_pairs = rdd.mapToPair(new PairFunction<POI_Review,String,POI_Review>(){
			private static final long serialVersionUID = 1L;

			public Tuple2<String, POI_Review> call(POI_Review arg0) throws Exception {
				return new Tuple2<String, POI_Review>(arg0.user_career,arg0);
			}
			
		});
		
		JavaPairRDD<String, Iterable<POI_Review>> group_result = POI_carrer_pairs.groupByKey();
		JavaPairRDD<String, List<POI_Review>> sample_result = group_result.mapToPair(
				new PairFunction<Tuple2<String,Iterable<POI_Review>>,String,List<POI_Review>>(){

					private static final long serialVersionUID = 1L;

					public Tuple2<String, List<POI_Review>> call(Tuple2<String, Iterable<POI_Review>> arg0)
							throws Exception {
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
						return new Tuple2<String,List<POI_Review>>(arg0._1,list);
					}
				});
		return sample_result;
	}
}
