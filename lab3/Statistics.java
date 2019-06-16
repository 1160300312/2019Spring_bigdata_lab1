package lab3;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Statistics {
	Master<Integer,Integer> master;
	
	public Statistics(Master<Integer,Integer> master){
		this.master = master;
	}
	
	public void getEdgeNum(){
		for(int i=0;i<master.workers.size();i++){
			Iterator<Map.Entry<Integer, List<Edge<Integer>>>> it = master.workers.get(i).edges.entrySet().iterator();
			int sum = 0;
			while(it.hasNext()){
				Map.Entry<Integer, List<Edge<Integer>>> entry = it.next();
				sum += entry.getValue().size();
			}
			System.out.println("worker id:" + master.workers.get(i).id + "\tedge number:" + sum);
		}
	}
	
	public void getVertexNum(){
		for(int i=0;i<master.workers.size();i++){
			VertexCountAggregator vca = new VertexCountAggregator();
			for(int j=0;j<master.workers.get(i).vertexes.size();j++){
				vca.report(master.workers.get(i).vertexes.get(j));
			}
			System.out.println("worker id:" + master.workers.get(i).id + "\tvertex number:" + vca.aggregate());
		}
	}
	
	public void getBSPMessage(){
		for(int i=0;i<master.workers.size();i++){
			for(int j=0;j<master.workers.get(i).use_time.size();j++){
				System.out.println("worker id:" + master.workers.get(i).id + "\tsuperstep" + j + "\tuse time:" + master.workers.get(i).use_time.get(j) + "ms");
				System.out.println("worker id:" + master.workers.get(i).id + "\tsuperstep" + j + "\tmessage exchange times:" + master.workers.get(i).message_send_num.get(j));
			}
		}
	}
}
