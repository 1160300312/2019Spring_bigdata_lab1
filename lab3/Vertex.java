package lab3;

import java.util.List;
import java.util.Map;
import java.util.Queue;

public abstract class Vertex<V,E> {
	private V value; //顶点的值
	private int id; //顶点的id
	private boolean active; //是否活跃
	
	public Vertex(int id){
		this.id = id;
	}

	public V getValue(){
		return this.value;
	}
	
	public void setValue(V new_value){
		this.value = new_value;
	}
	
	public int getId(){
		return this.id;
	}
	
	public boolean isActive(){
		return this.active;
	}
	
	public void setActive(){
		this.active = true;
	}
	
	public void voteToHalt(){
		this.active = false;
	}
	
	public abstract void compute(Queue<Message> msg);
	
	public abstract Map<Integer, Message> getMessage(List<Edge<E>> edges);
}
