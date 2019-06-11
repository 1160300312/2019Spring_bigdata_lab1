package lab3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class SSSPWorker extends Worker<Integer,Integer>{
	public SSSPWorker(int id) {
		super(id);
	}

	public void load(){
		String path = "part " + this.id;
		FileReader fr;
		try {
			fr = new FileReader(path);
			BufferedReader br = new BufferedReader(fr);
			int size_vertex = Integer.parseInt(br.readLine());
			for(int i=0;i<size_vertex;i++){
				int id = Integer.parseInt(br.readLine());
				SSSPVertex v = new SSSPVertex(id);
				v.setValue(INF);
				this.vertexes.add(v);
				this.edges.put(id, new ArrayList<Edge<Integer>>());
			}
			int size_edge = Integer.parseInt(br.readLine());
			for(int i=0;i<size_edge;i++){
				String line = br.readLine();
				//System.out.println(line);
				String[] words = line.split("\t");
				this.edges.get(Integer.parseInt(words[0])).add(new Edge<Integer>(1,Integer.parseInt(words[1])));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
