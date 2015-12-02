package Kmeans;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class KmeansCombiner extends Reducer<KcodeWritable, Text, KcodeWritable, Text>
{
	@Override
	public void reduce(KcodeWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
		int count = 0;
		Kvector sum = null;
		
		for (Text value : values)
		{
			Kvector temp = Kvector.fromString(value.toString());
			if (sum == null)
				sum = temp;
			else
				sum.add(temp);
			count += temp.code;
		}
		sum.code = count;
		
		context.write(key, new Text(sum.toString()));
	}
}
