

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> vals, Context context){
		long pass = context.getCounter(PageRankEnum.PASS).getValue();
		if (pass == 0){
			String toList = "";
			for (Text val : vals){
				toList += "," + val.toString();
			}
			if (toList.length() > 0){
				toList = toList.substring(1);
			}
			try {
				context.write(key, new Text(toList + "|" + CONST.BASE_PAGE_RANK));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			context.getCounter(PageRankEnum.TOTAL_NODES).increment(1);
		} else {
			double redistributeValue = context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).getValue()/(context.getCounter(PageRankEnum.TOTAL_NODES).getValue() * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG);
			double newPageRank = CONST.RANDOM_SURFER*CONST.BASE_PAGE_RANK + redistributeValue;
			double oldPageRank = 0.;
			String toList = "";
			for (Text val : vals){
				if (val.toString().contains("|")){
					String[] info = val.toString().split("|");
					toList = info[0];
					oldPageRank = Double.parseDouble(info[1]);
				} else {
					newPageRank += CONST.DAMPING_FACTOR * Double.parseDouble(val.toString());
				}
				
			}
			double residualValue = Math.abs(newPageRank - oldPageRank)/newPageRank;
			context.getCounter(PageRankEnum.RESIDUAL_SUM).increment((long)(residualValue * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5));
			try {
				context.write(key, new Text(toList + "|" + newPageRank + "|" + residualValue));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
