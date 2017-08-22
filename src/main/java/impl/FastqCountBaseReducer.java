package impl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FastqCountBaseReducer 
  extends Reducer<Text, LongWritable, Text, LongWritable> {
  
    public void reduce(Text key, Iterable<LongWritable> values,  Context context) throws IOException, InterruptedException {
	   long total = 0;
	   for(LongWritable val: values){
		  total += val.get();
	   }
	   context.write(key,new LongWritable(total));
    }
}

