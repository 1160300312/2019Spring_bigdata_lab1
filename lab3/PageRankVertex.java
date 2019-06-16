package lab3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class PageRankVertex extends Vertex<Double,Integer>{

	public PageRankVertex(int id) {
		super(id);
	}

	@Override
	public void compute(Queue<Message> msg) {
		Message m;
		double sum = 0;
		int flag = 0;
		while((m=msg.poll())!=null){
			sum += m.pagerankvalue;
			flag = 1;
		}
		if(flag == 1)
			this.setValue(sum);
	}

	@Override
	public Map<Integer, Message> getMessage(List<Edge<Integer>> edges) {
		Map<Integer, Message> result = new HashMap<Integer, Message>();
		for(Edge<Integer> e:edges){
			Message new_msg = new Message();
			new_msg.pagerankvalue = this.getValue()/edges.size()*0.85 + 0.15;
			result.put(e.des, new_msg);
		}
		return result;
	}

}
