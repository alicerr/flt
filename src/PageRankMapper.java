

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	public void mapper(IntWritable keyin, Text val, Context context){
		long pass = context.getCounter(PageRankEnum.PASS).getValue();
		if (pass == 0){
			String[] info = val.toString().split("\t");
			try {
				context.write(new IntWritable(Integer.parseInt(info[0])), new Text(info[2]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} else {
			String[] info = val.toString().split("|");
			Integer from = Integer.parseInt(info[0]);
			String[] toList = info[1].split(",");
			Double pr = Double.parseDouble(info[2]);
			if (toList.length == 0){
				context.getCounter(PageRankEnum.SINKS_TO_REDISTRIBUTE).increment(
						(long)(pr * CONST.SIG_FIG_FOR_DOUBLE_TO_LONG + .5)
						);
			} else {
				for (String to : toList){
					int toID = Integer.parseInt(to);
					try {
						context.write(new IntWritable(toID), new Text(Double.toString(pr/toList.length)));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
							
				}
				try {
					context.write(new IntWritable(from), new Text(info[1] + "|" + Double.toString(pr)));
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
}
