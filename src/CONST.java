

/**
 * Constants used in Node, block, and Gauss pagerank runs
 * @author Alice, Spencer, Garth
 *
 */
public abstract class CONST {
	public static final float TOTAL_NODES = Util.blocks[Util.blocks.length -1];
	public static final float BASE_PAGE_RANK = 1/TOTAL_NODES;
	public static final float DAMPING_FACTOR = .85f;
	public static final float RANDOM_SURFER = 1 - DAMPING_FACTOR;
	public static final float SIG_FIG_FOR_float_TO_LONG = 10000L;
	public static final float SIG_FIG_FOR_TINY_float_TO_LONG = 10000000000L;
	
	public static final byte SEEN_NODE_MARKER = 0,
							 SEEN_EDGE_MARKER = 1,
							 ENTIRE_BLOCK_DATA_MARKER = 2,
							 INCOMING_EDGE_MARKER = 3;
	public static final int MARKER_INDEX_L0 = 0,
			                NODE_LIST = 1,
			                  EDGE_TO = 1,
			                  EDGE_FROM = 0,
			                INNER_EDGE_LIST = 2,
			                OUTER_EDGE_LIST = 3;
	public static final String L0_DIV = "U",
							   L1_DIV = ":",
							   L2_DIV = ",";
	public static final float RESIDUAL_SUM_DELTA = 0.001f;
	
			                
			                
			                
			                
			                
							 

}
