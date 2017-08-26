package impl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;


public class FastqReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static final Logger LOG = Logger.getLogger(FastqReducer.class);

    public void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {

	   	Integer total = 0;
	   	for(IntWritable val: values){
	   		total += val.get();
	   	}

		LOG.info("key = " + key + ", value: " + total);

	   	if(total > 1) {
			context.write(key, new IntWritable(total));
		}

    }
}

