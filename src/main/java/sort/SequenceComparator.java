package sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SequenceComparator extends WritableComparator {

    public SequenceComparator() {
        super(SequencePair.class, true);
    }

    @Override
    public int compare(WritableComparable sp1, WritableComparable sp2) {
        SequencePair sequencePair = (SequencePair) sp1;
        SequencePair sequencePair2 = (SequencePair) sp2;
        return -1 * sequencePair.compareTo(sequencePair2);
    }
}

