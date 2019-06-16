package lab3;

import java.util.List;

public interface Aggregator<V,E,R> {
	
	public void report(Vertex<V,E> v);
	
	public R aggregate();
}
