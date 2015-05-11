import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Implements the first Job reduce functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class BlockReducerPass0 extends
		Reducer<LongWritable, BytesWritable, LongWritable, BytesWritable> {
	/** Overrides the Reduce function
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(LongWritable key, Iterable<BytesWritable> vals, Context context){
		
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		int max = Util.baseValue((byte) (key.get() + 1));
		for (int i = Util.baseValue((byte) key.get()); i < max; i++){
			Node n = new Node(i);
			n.setPR(CONST.BASE_PAGE_RANK);
			nodes.put(i, n);
		}
			
		HashMap<Integer, ArrayList<Edge>> i = new HashMap<Integer, ArrayList<Edge>>();

		HashMap<Integer, ArrayList<Edge>> o = new HashMap<Integer, ArrayList<Edge>>();
		int count = 0;
		
		// Loops through all the values associated with the key
		for (BytesWritable val : vals){
			count++;
			// Each value is checked if we have seen it, seen the edge, and add the node
			// to seenNodes and nodes appropriately
			ByteBuffer b = ByteBuffer.wrap(val.getBytes());
			int from = b.getInt();
			int to = b.getInt();
			nodes.get(from).addBranch();
			// Specifies if we are currently looking at an internal edge or external edge for a block
			if (Util.idToBlock(to) == key.get()){
				if (i.containsKey(to)){
					i.get(to).add(new Edge(from, to));
				} else {
					ArrayList<Edge> ae = new ArrayList<Edge>();
					ae.add(new Edge(from, to));
					i.put(to,  ae);
				}
			} else {
				if (o.containsKey(from)){
					o.get(from).add(new Edge(from, to));
				} else {
					ArrayList<Edge> ae = new ArrayList<Edge>();
					ae.add(new Edge(from, to));
					o.put(from,  ae);
				}
			}
		}
		System.out.println(count + " recieved, " + nodes.size() + " " + i.size() + " " + o.size());
		// Add dividers as necessary for empty strings
		
		
		try {
			// Write out block data for next job
			context.write(key, new BytesWritable(Util.blockToByteBuffer(nodes, i, o).array()));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

			System.out.println(e);
		}

		 
	}


}
