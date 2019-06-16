package lab3;

import java.util.ArrayList;
import java.util.List;

public class VertexCountAggregator implements Aggregator<Integer,Integer,Integer>{
	
	List<Integer> data;
	
	public VertexCountAggregator(){
		this.data = new ArrayList<Integer>();
	}

	@Override
	public void report(Vertex<Integer, Integer> v) {
		this.data.add(1);
	}

	@Override
	public Integer aggregate() {
		return this.data.size();
	}

}
