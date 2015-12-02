package Kmeans;

public class Kvector
{
	public int[] array;
	public int code;
	
	public Kvector(int[] a, int c)
	{
		array = a;
		code = c;
	}
	
	public static Kvector fromString(String str)
	{
		String[] items = str.split("\t");
		int n = items.length;
		int[] arr = new int[n-1];
		for (int i = 1; i < n; ++i)
			arr[i-1] = Integer.parseInt(items[i]);
		return new Kvector(arr, Integer.parseInt(items[0]));
	}
	
	public static String toString(Kvector v)
	{
		StringBuilder str = new StringBuilder();
		str.append(v.code);
		int n = v.array.length;
		for (int i = 0; i < n; ++i)
			str.append("\t").append(v.array[i]);
		return str.toString();
	}
	
	public int distance(Kvector v)
	{
		int n = v.array.length;
		assert(array.length != n);
		
		double d = 0.0;
		for (int i = 0; i < n; ++i)
		{
			int t = array[i] - v.array[i];
			d += t * t;
		}
		
		return (int)Math.sqrt(d);
	}
	
	public void add(Kvector v)
	{
		int n = array.length;
		for (int i = 0; i < n; ++i)
			array[i] += v.array[i];
	}
	
	public void divide(int d)
	{
		int n = array.length;
		for (int i = 0; i < n; ++i)
			array[i] /= d;
	}
	
	@Override
	public String toString()
	{
		return Kvector.toString(this);
	}
}
