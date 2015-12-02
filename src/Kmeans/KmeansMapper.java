package Kmeans;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

class KmeansMapper extends Mapper<LongWritable, Text, KcodeWritable, Text>
{
	private List<Kvector> m_kVectors = new LinkedList<Kvector>();
	
	@Override
	public void setup(Context context) throws IOException
	{
		Configuration conf = context.getConfiguration();
		
		int num = Integer.parseInt(conf.get("kvNum"));
		while (--num >= 0)
		{
			String s = conf.get(String.valueOf(num));
			m_kVectors.add(Kvector.fromString(s));
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		String str = "1\t" + value.toString();
		Kvector vector = Kvector.fromString(str);
		
		int code = -1, d = Integer.MAX_VALUE;
		for (Kvector e : m_kVectors)
		{
			int t = vector.distance(e);
			if (t < d) { d = t; code = e.code; }
		}
		
		context.write(new KcodeWritable(code), new Text(str));
	}
}
