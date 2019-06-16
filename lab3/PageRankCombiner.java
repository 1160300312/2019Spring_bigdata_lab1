package lab3;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class PageRankCombiner implements Combiner{
	
	@Override
	public Map<Integer, Message> combine(Map<Integer, Queue<Message>> input) {
		Map<Integer, Message> result = new HashMap<Integer,Message>();
		Iterator<Map.Entry<Integer, Queue<Message>>> it = input.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Integer, Queue<Message>> entry = it.next();
			Queue<Message> data = entry.getValue();
			Message m;
			double sum = 0;
			while((m=data.poll())!=null){
				sum += m.pagerankvalue;
			}
			Message re = new Message();
			re.pagerankvalue = sum;
			result.put(entry.getKey(), re);
		}
		
		return result;
	}
}
