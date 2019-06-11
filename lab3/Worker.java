package lab3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class Worker<V,E> {
	int id;
	private boolean use_combiner = false;
	private Combiner combiner;
	
	List<Vertex<V,E>> vertexes;
	Map<Integer, List<Edge<E>>> edges;
	Map<Integer, Queue<Message>> last_msg;
	Map<Integer, Queue<Message>> current_msg;
	
	Map<Integer, Queue<Message>> send_queue;
	
	final int INF = 6000000;
	
	public Worker(int id){
		this.id = id;
		this.vertexes = new ArrayList<Vertex<V,E>>();
		this.edges = new HashMap<Integer,List<Edge<E>>>();
	}
	
	public void init(){
		last_msg = new HashMap<Integer, Queue<Message>>();
		for(Vertex<V,E> v:vertexes){
			last_msg.put(v.getId(), new LinkedBlockingQueue<Message>());
		}
		current_msg = new HashMap<Integer, Queue<Message>>();
		for(Vertex<V,E> v:vertexes){
			current_msg.put(v.getId(), new LinkedBlockingQueue<Message>());
		}
		if(last_msg.containsKey(0)){
			Message m = new Message();
			m.value = 0;
			last_msg.get(0).add(m);
		}
		send_queue = new HashMap<Integer, Queue<Message>>();
	}
	 
	
	public void run(Master<V,E> master){
		for(Vertex<V,E> v: vertexes){
			//收到其他顶点信息的顶点设为avtive
			if(last_msg.get(v.getId()).size() != 0){
				v.setActive();
			}
			if(v.isActive()){
				//运行活跃的顶点
				v.compute(this.last_msg.get(v.getId()));
				//获取顶点要发送的消息
				Map<Integer, Message> msgs = v.getMessage(edges.get(v.getId()));
				//消息传递
				Iterator<Map.Entry<Integer, Message>> it = msgs.entrySet().iterator();
				if(!this.use_combiner){
					while(it.hasNext()){
						Map.Entry<Integer, Message> entry = it.next();
						if(current_msg.containsKey(entry.getKey())){
							current_msg.get(entry.getKey()).add(entry.getValue());
						} else{
							Worker<V,E> w = master.findWorker(entry.getKey());
							if(w != null){
								w.current_msg.get(entry.getKey()).add(entry.getValue());
							} else{
								System.out.println("cannot find vertex " + entry.getKey());
							}
						}
					}
				} else{
					while(it.hasNext()){
						Map.Entry<Integer, Message> entry = it.next();
						if(this.send_queue.containsKey(entry.getKey())){
							this.send_queue.get(entry.getKey()).add(entry.getValue());
						} else{
							this.send_queue.put(entry.getKey(), new LinkedBlockingQueue<Message>());
							this.send_queue.get(entry.getKey()).add(entry.getValue());
						}
					}
				}
			} 
			if(this.use_combiner){
				Map<Integer,Message> combine_result = combiner.combine(send_queue);
				Iterator<Map.Entry<Integer, Message>> it = combine_result.entrySet().iterator();
				while(it.hasNext()){
					Map.Entry<Integer, Message> entry = it.next();
					if(current_msg.containsKey(entry.getKey())){
						current_msg.get(entry.getKey()).add(entry.getValue());
					} else{
						Worker<V,E> w = master.findWorker(entry.getKey());
						if(w != null){
							w.current_msg.get(entry.getKey()).add(entry.getValue());
						} else{
							System.out.println("cannot find vertex " + entry.getKey());
						}
					}
				}
			}
		}
	}
	
	public void finishABSP(){
		//下一轮BSP初始化消息队列
		for(Vertex<V,E> v : this.vertexes){
			if(last_msg.get(v.getId()).size() != 0){
				v.voteToHalt();
			}
		}
		last_msg = current_msg;
		current_msg = new HashMap<Integer, Queue<Message>>();
		for(Vertex<V,E> v:vertexes){
			current_msg.put(v.getId(), new LinkedBlockingQueue<Message>());
		}
	}
	
	public boolean isDone(){
		boolean result = true;
		for(Vertex<V,E> v: vertexes){
			if(v.isActive()){
				result = false;
			}
		}
		return result;
	}
	
	public abstract void load();
	
	public void useCombiner(Combiner combiner){
		this.use_combiner = true;
		this.combiner = combiner;
	}
}

class Edge<E>{
	E value;
	int des;
	
	public Edge(E value, int des){
		this.value = value;
		this.des = des;
	}
}
