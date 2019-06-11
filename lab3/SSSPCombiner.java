package lab3;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class SSSPCombiner implements Combiner{
	
	final int INF = 6000000;

	@Override
	public Map<Integer, Message> combine(Map<Integer, Queue<Message>> input) {
		Map<Integer, Message> result = new HashMap<Integer,Message>();
		Iterator<Map.Entry<Integer, Queue<Message>>> it = input.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Integer, Queue<Message>> entry = it.next();
			Queue<Message> data = entry.getValue();
			Message m;
			Message min = new Message();
			min.value = INF;
			while((m=data.poll())!=null){
				if(m.value < min.value){
					min.value = m.value;
				}
			}
			result.put(entry.getKey(), min);
		}
		return result;
	}

}
