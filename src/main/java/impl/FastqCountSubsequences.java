package impl;

import fastq.BaseComparator;
import fastq.BasePartitioner;
import fastq.FastqInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FastqCountSubsequences extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(FastqCountSubsequences.class);

	public int run(String [] args) throws Exception {

		logger.info("run(): input  args[0]=" + args[0]);
		logger.info("run(): output args[1]=" + args[1]);

		Configuration conf = new Configuration();
		if(args.length == 3) {
			conf.set("length", args[2]);
			logger.info("run(): length args[2]=" + args[2]);
		}

		Job job = Job.getInstance(conf, "Fastq count");
		job.setJarByClass(FastqCountSubsequences.class);
		job.setJobName("FastqCountSubsequences");
		
		job.setInputFormatClass(FastqInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setPartitionerClass(BasePartitioner.class);
		job.setSortComparatorClass(BaseComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(FastqMapper.class);
		job.setCombinerClass(FastqReducer.class);
		job.setReducerClass(FastqReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		if(args[1].equals("output/")) {
			FileSystem.get(getConf()).delete(new Path(args[1]), true);
			TextOutputFormat.setOutputPath(job, new Path(args[1]));
		}

		boolean status = job.waitForCompletion(true);
		logger.info("run(): status="+status);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			logger.info("GRESKA: Pogresan broj ulaznih parametara: <input-dir> <output-dir> <length>");
			throw new IllegalArgumentException("GRESKA: Pogresan broj ulaznih parametara: <input-dir> <output-dir> <length>");
		}

		int ret = ToolRunner.run(new FastqCountSubsequences(), args);
		System.exit(ret);
	}
}
