package impl;

import fastq.BaseComparator;
import fastq.BasePartitioner;
import fastq.FastqInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import sort.*;

public class FastqCountSubsequences extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(FastqCountSubsequences.class);

	public int run(String [] args) throws Exception {

		String INPUT_PATH = args[0];
		String TMP_PATH = args[1];
		String OUTPUT_PATH = args[2];

		logger.info("run(): input  args[0]=" + INPUT_PATH);
		logger.info("run(): tmp args[1]=" + TMP_PATH);
		logger.info("run(): output args[2]=" + OUTPUT_PATH);

		Configuration conf = new Configuration();
		if(args.length == 4) {
			conf.set("length", args[3]);
			logger.info("run(): length args[3]=" + args[3]);
		}

		Job job = Job.getInstance(conf, "Fastq count");
		job.setJarByClass(FastqCountSubsequences.class);
		job.setInputFormatClass(FastqInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setPartitionerClass(BasePartitioner.class);
		job.setSortComparatorClass(BaseComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(FastqMapper.class);
		job.setCombinerClass(FastqReducer.class);
		job.setReducerClass(FastqReducer.class);

		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));

		if(TMP_PATH.equals("output/")) {
			FileSystem.get(getConf()).delete(new Path(TMP_PATH), true);
			TextOutputFormat.setOutputPath(job, new Path(TMP_PATH));
		}

		boolean status = job.waitForCompletion(true);

		if(status) {
			Job job1 = Job.getInstance(conf, "Fastq sort");
			job1.setJarByClass(FastqCountSubsequences.class);
			job1.setInputFormatClass(KeyValueTextInputFormat.class);
			job1.setOutputKeyClass(SequencePair.class);
			job1.setOutputValueClass(NullWritable.class);
			job1.setMapperClass(SortingMapper.class);
			job1.setPartitionerClass(SequencePartitioner.class);
			job1.setSortComparatorClass(SequenceComparator.class);
			job1.setGroupingComparatorClass(GroupComparator.class);
			job1.setReducerClass(SortingReducer.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job1, new Path(TMP_PATH));
			if (OUTPUT_PATH.equals("outputSort/")) {
				FileSystem.get(getConf()).delete(new Path(OUTPUT_PATH), true);
				TextOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));
			}

			job1.setOutputFormatClass(TextOutputFormat.class);
			status = job1.waitForCompletion(true);
		}

		logger.info("run(): status="+status);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			logger.info("GRESKA: Pogresan broj ulaznih parametara: <input-dir> <tmp-dir> <output-dir> <length>");
			throw new IllegalArgumentException("GRESKA: Pogresan broj ulaznih parametara: <input-dir> <tmp-dir> <output-dir> <length>");
		}

		int ret = ToolRunner.run(new FastqCountSubsequences(), args);
		System.exit(ret);
	}
}
