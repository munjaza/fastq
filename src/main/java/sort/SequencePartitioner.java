package sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SequencePartitioner extends Partitioner<SequencePair, NullWritable> {

    @Override
    public int getPartition(SequencePair sequencePair, NullWritable nullWritable, int numPartitions) {
        return (sequencePair.getSubsequence().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}