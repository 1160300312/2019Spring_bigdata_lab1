package lab3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class SSSPVertex extends Vertex<Integer,Integer>{
	
	public SSSPVertex(int id) {
		super(id);
	}


	final int INF = 6000000;

	@Override
	public void compute(Queue<Message> msg) {
		Message m;
		int flag = 0;
		while((m=msg.poll())!=null){
			if(this.getValue() > m.value){
				this.setValue(m.value);
				//System.out.println(m.value);
				flag = 1;
			}
		}
		if(flag == 0){
			//System.out.println(this.getId() + " halt");
			this.voteToHalt();
		} else {
			this.setActive();
			//System.out.println(this.getId() + " active");
		}
	}


	@Override
	public Map<Integer, Message> getMessage(List<Edge<Integer>> edges) {
		Map<Integer, Message> result = new HashMap<Integer, Message>();
		//只有在当前节点的值不是INF的时候才传递消息
		if(this.getValue()!=INF){
			for(Edge<Integer> e:edges){
				Message new_msg = new Message();
				new_msg.value = e.value + this.getValue();
				result.put(e.des, new_msg);
			}
		}
		return result;
	}
}
