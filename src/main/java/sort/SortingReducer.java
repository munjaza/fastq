package sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SortingReducer extends Reducer<SequencePair, NullWritable, SequencePair, NullWritable> {

    public void reduce(SequencePair pair, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

    	context.write(pair, NullWritable.get());
    }
}

