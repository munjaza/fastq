package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public  class SortingMapper extends Mapper<Text, Text, SequencePair, NullWritable>{

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		context.write(new SequencePair(key, new IntWritable(Integer.parseInt(value.toString()))), NullWritable.get());
	}
}