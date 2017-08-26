package impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FastqMapper extends Mapper<Object, Text, Text, IntWritable> {
    
    private Map<String, Integer> dnaBaseCounter = null;
    private IntWritable counter = new IntWritable();
	private Text base = new Text();

    protected void setup(Context context) throws IOException, InterruptedException {
       dnaBaseCounter = new HashMap<String, Integer>();
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<String, Integer> entry : dnaBaseCounter.entrySet()) {
           base.set(entry.getKey());
           counter.set(entry.getValue());
           context.write(base, counter);      
        }
    }
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	   	String fastqRecord = value.toString();
	   	String[] lines = fastqRecord.split(",;,");
	   	String sequence = lines[1].toLowerCase();
	   	List<String> subSequences = new ArrayList<String>();

		Configuration conf = context.getConfiguration();
		Integer length;
		if(conf.get("length") != null) {
			length = Integer.valueOf(conf.get("length"));

			for (int i = 0; i <= sequence.length() - length; i++) {
				String s = sequence.substring(i, i + length);
				if (s.matches("[a-zA-Z]+")) {
					subSequences.add(s);
				}
			}
			for (String s : subSequences) {
				Integer count = dnaBaseCounter.get(s);
				if (count == null) {
					dnaBaseCounter.put(s, 1);
				} else {
					dnaBaseCounter.put(s, count + 1);
				}
			}
		}
	}
}
