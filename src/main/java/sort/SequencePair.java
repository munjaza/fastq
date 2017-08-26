package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SequencePair implements WritableComparable<SequencePair> {

    private Text subsequence;
    private IntWritable counter;

    public SequencePair()
    {
        this.subsequence = new Text();
        this.counter = new IntWritable();
    }

    public SequencePair(Text subsequence, IntWritable counter) {
        this.subsequence = subsequence;
        this.counter = counter;
    }

    public int compareTo(SequencePair sequencePair) {

        int compareValue = this.counter.compareTo(sequencePair.getCounter());
        if (compareValue == 0) {
            compareValue = subsequence.compareTo(sequencePair.getSubsequence());
        }
        return compareValue;
    }

    public Text getSubsequence() {
        return subsequence;
    }

    public IntWritable getCounter() {
        return counter;
    }

    @Override
    public String toString() {
        return (new StringBuilder().append(subsequence).append("\t").append(counter)).toString();
    }

    public void readFields(DataInput dataInput) throws IOException {
        subsequence.readFields(dataInput);
        counter.readFields(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        subsequence.write(dataOutput);
        counter.write(dataOutput);
    }
}
