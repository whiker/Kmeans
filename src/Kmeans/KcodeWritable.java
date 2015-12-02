package Kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KcodeWritable implements WritableComparable<Object>
{
	private int code;
	
	public KcodeWritable()      { code = -1; }
	public KcodeWritable(int c)	{ code = c; }
	public int get()            { return code; }

	@Override
	public void readFields(DataInput in) throws IOException
	{ code = in.readInt(); }

	@Override
	public void write(DataOutput out) throws IOException
	{ out.writeInt(code); }

	@Override
	public int compareTo(Object o)
	{ return 0; }

	public static class Comparator extends WritableComparator
	{
		public Comparator() { super(KcodeWritable.class); }
		
		public int compare(byte[] b1, int s1, int l1,
			byte[] b2, int s2, int l2)
		{ return 0; }
	}
	
	static { WritableComparator.define(KcodeWritable.class, new Comparator()); }
}
