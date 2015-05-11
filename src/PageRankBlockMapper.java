import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Implements the subsequent Jobs (after first pass) map functionality for BlockMain.java
 * @author Alice, Spencer, Garth
 *
 */
public class PageRankBlockMapper extends
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {
	
/** Overrides map
 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
 */
public void map(LongWritable keyin, BytesWritable val, Context context){
		
		// Create the hashmaps we will be using. TODO: if this was implemented we would not want to 
	    System.out.println(keyin + " recieved");
		HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();
		HashMap<Integer, ArrayList<Edge>> outerEdges = new HashMap<Integer, ArrayList<Edge>>();
		//we don't need the inner edges
		HashMap<Integer, ArrayList<Edge>> innerEdges = null;
		float sinks = Util.fillBlockFromByteBuffer(ByteBuffer.wrap(val.getBytes()), nodes, innerEdges, outerEdges);
		

		// Handle Sinks
		context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment((long) (sinks * CONST.SIG_FIG_FOR_TINY_float_TO_LONG + .5));
		
		// For each outer edge send a value
		for (ArrayList<Edge> ae : outerEdges.values()){
			for (Edge e : ae){
				try { // Get all the PR values from edges of block
					context.write(new LongWritable(Util.idToBlock(e.to)), new BytesWritable(Util.incomingValue(e.to, nodes.get(e.from))));
				} catch (IOException | InterruptedException e1) {
					// TODO Auto-generated catch block
					System.out.println(e);
					e1.printStackTrace();
				}
			}
		}
		
		try { // Write keyin and val for next round
			context.write(keyin, val);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
	
	}
}
	

