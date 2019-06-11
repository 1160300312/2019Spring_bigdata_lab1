package lab3;

import java.util.Map;
import java.util.Queue;

public interface Combiner {
	public Map<Integer, Message> combine(Map<Integer, Queue<Message>> input);
}
