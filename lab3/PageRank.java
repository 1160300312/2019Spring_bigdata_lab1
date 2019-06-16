package lab3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;

public class PageRank extends Master<Double,Integer>{

		@Override
		public void run(int k) {
		for(int i=0;i<k;i++){
			PageRankWorker worker = new PageRankWorker(i);
			worker.load();
			worker.init();
			this.workers.add(worker);
			if(this.use_combiner){
				worker.useCombiner(this.combiner);
			}
		}
		int loop = 10;
		int count = 0;
		while(count < loop){
			System.out.println(count);
			for(Worker<Double, Integer> w : workers){
				w.run(this);
			}
			for(Worker<Double,Integer> w : workers){
				w.finishABSP();
			}	
			count++;
		}
		
	}
	
	public void writeResult(String filepath){
		FileWriter fw;
		 DecimalFormat df = new DecimalFormat("#.0000");
		try {
			fw = new FileWriter(filepath);
			BufferedWriter out = new BufferedWriter(fw);
			for(int i=0;i<this.workers.size();i++){
				for(int j=0;j<this.workers.get(i).vertexes.size();j++){
					out.write(this.workers.get(i).vertexes.get(j).getId() + "\t" + df.format(this.workers.get(i).vertexes.get(j).getValue()) + "\n");
				}
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]){
		PageRank p = new PageRank();
		p.run(8);
		p.writeResult("PageRank_Result");
	}
}
