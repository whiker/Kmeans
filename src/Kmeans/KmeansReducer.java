package Kmeans;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

class KmeansReducer extends Reducer<KcodeWritable, Text, NullWritable, Text>
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
		sum.code = key.get();
		
		sum.divide(count);
		
		context.write(null, new Text(sum.toString()));
	}
}
