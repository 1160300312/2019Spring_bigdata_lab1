package lab3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class SSSP extends Master<Integer,Integer>{
	final int INF = 6000000;
	
	@Override
	public void run(int k){
		Statistics st = new Statistics(this);
		for(int i=0;i<k;i++){
			SSSPWorker worker = new SSSPWorker(i);
			worker.load();
			worker.init();
			this.workers.add(worker);
			if(this.use_combiner){
				worker.useCombiner(this.combiner);
			}
		}
		st.getEdgeNum();
		st.getVertexNum();
		while(true){
			for(Worker<Integer, Integer> w : workers){
				w.run(this);
			}
			if(this.stop()){
				break;
			}
			for(Worker<Integer,Integer> w : workers){
				w.finishABSP();
			}	
		}
		st.getBSPMessage();
	}
	
	public void writeResult(String filepath){
		
		FileWriter fw;
		try {
			fw = new FileWriter(filepath);
			BufferedWriter out = new BufferedWriter(fw);
			for(int i=0;i<this.workers.size();i++){
				for(int j=0;j<this.workers.get(i).vertexes.size();j++){
					if(this.workers.get(i).vertexes.get(j).getValue() != INF)
						out.write(this.workers.get(i).vertexes.get(j).getId() + "\t" + this.workers.get(i).vertexes.get(j).getValue() + "\n");
					else
						out.write(this.workers.get(i).vertexes.get(j).getId() + "\t" + "INF" + "\n");
				}
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]){
		SSSP master = new SSSP();
		//master.useCombiner(new SSSPCombiner());
		master.run(8);
		//master.partition("test.txt", 2);
		master.writeResult("SSSP_result_test");
	}
}
