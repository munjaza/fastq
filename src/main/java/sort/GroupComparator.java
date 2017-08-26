package sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

    protected GroupComparator() {
        super(SequencePair.class, true);
    }

    @Override
    public int compare(WritableComparable sp1, WritableComparable sp2) {
        SequencePair sequencePair = (SequencePair) sp1;
        SequencePair sequencePair2 = (SequencePair) sp2;
        return sequencePair.getSubsequence().compareTo(sequencePair2.getSubsequence());
    }
}