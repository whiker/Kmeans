package Kmeans;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

class KmeansJob extends Configured implements Tool
{
	public String inputPath;
	public String outputPath;
	
	private Configuration m_conf = new Configuration();
	private int m_knum;
	
	public void setKvector(List<Kvector> kVectors, int knum) throws IOException
	{
		for (Kvector e : kVectors)
			m_conf.set(String.valueOf(e.code), e.toString());
		
		m_knum = knum;
		m_conf.set("kvNum", String.valueOf(knum));
	}
	
	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(m_conf);
		job.setJarByClass(KmeansJob.class);
		job.setJobName("LogHandle");
		
		job.setMapOutputKeyClass(KcodeWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(m_knum);
		
		job.setMapperClass(KmeansMapper.class);
		job.setCombinerClass(KmeansCombiner.class);
		job.setReducerClass(KmeansReducer.class);
		
		job.setPartitionerClass(MyPartition.class);
		
		//job.setSortComparatorClass(MyComparator.class);
		//job.setGroupingComparatorClass(MyComparator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	
	public static class MyComparator implements RawComparator<Object>
	{
		@Override
		public int compare(byte[] a1, int a2, int a3,
				byte[] b1, int b2, int b3)
		{ return 0; }

		@Override
		public int compare(Object o1, Object o2)
		{ return 0; }
	}

	public static class MyPartition extends Partitioner<KcodeWritable, Text>
	{
		@Override
		public int getPartition(KcodeWritable key, Text value, int numPartition)
		{ return key.get(); }
	}
}
