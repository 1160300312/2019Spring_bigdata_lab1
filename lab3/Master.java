package lab3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

public abstract class Master<V,E> {
	List<Worker<V,E>> workers;
	boolean use_combiner = false;
	Combiner combiner;
	
	public Master(){
		this.workers = new ArrayList<Worker<V,E>>();
	}
	
	public abstract void run(int k);
	
	public boolean stop(){
		boolean result = true;
		for(Worker<V,E> w : workers){
				if(!w.isDone()){
					result = false;
			}
		}
		return result;
	}
	
	public Worker<V,E> findWorker(int id){
		for(Worker<V,E> w : workers){
			if(w.current_msg.containsKey(id)){
				return w;
			}
		}
		return null;
	}
	
	public void partition(String pathname, int k){
		List<HashSet<Integer>> partition_vertex = new ArrayList<HashSet<Integer>>();
		List<ArrayList<MyTuple2>> partition_edge = new ArrayList<ArrayList<MyTuple2>>();
		for(int i=0;i<k;i++){
			partition_vertex.add(new HashSet<Integer>());
			partition_edge.add(new ArrayList<MyTuple2>());
		}
		
		Random r = new Random();
		try {
			FileReader fr = new FileReader(pathname);
			BufferedReader br = new BufferedReader(fr);
			String str;
			for(int i=0;i<4;i++){
				br.readLine();
			}
			while((str = br.readLine()) != null){
				String[] words = str.split("\t");
				for(int i=0;i<words.length;i++){
					int flag = 0;
					for(int j=0;j<partition_vertex.size();j++){
						if(partition_vertex.get(j).contains(Integer.parseInt(words[i]))){
							if(i == 0){
								partition_edge.get(j).add(new MyTuple2(Integer.parseInt(words[i]), Integer.parseInt(words[i+1])));
							}
							flag = 1;
							break;
						}
					}
					if(flag == 1){
						continue;
					} else{
						int index = r.nextInt(k);
						partition_vertex.get(index).add(Integer.parseInt(words[i]));
						if(i == 0){
							partition_edge.get(index).add(new MyTuple2(Integer.parseInt(words[i]), Integer.parseInt(words[i+1])));
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Integer> partition_vertex_list;
		for(int i=0;i<k;i++){
			FileWriter fw;
			try {
				partition_vertex_list = new ArrayList<Integer>(partition_vertex.get(i));
				fw = new FileWriter("part " + i);
				BufferedWriter out = new BufferedWriter(fw);
				out.write(partition_vertex_list.size() + "\n");
				for(int j=0;j<partition_vertex_list.size();j++){
					out.write(partition_vertex_list.get(j) + "\n");
				}
				out.write(partition_edge.get(i).size() + "\n");
				for(int j=0;j<partition_edge.get(i).size();j++){
					out.write(partition_edge.get(i).get(j).src + "\t" + partition_edge.get(i).get(j).des + "\n");
				}
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void useCombiner(Combiner combiner){
		this.use_combiner = true;
		this.combiner = combiner;
	}
}

class MyTuple2{
	int src;
	int des;
	
	public MyTuple2(int src, int des){
		this.src = src;
		this.des = des;
	}
}
