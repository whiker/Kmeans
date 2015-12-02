package Kmeans;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Kmeans
{
	public String dataFilename;
	public int knum;
	public int dimension;           // 向量维数
	public int[] max, min;
	public int endError = 0;        // 终止误差
	public int iterateMax = 10000;  // 最大迭代次数
	
	private List<Kvector> m_kVectors;
	private String m_workspace;
	private FileSystem m_fs;
	private String[] m_resultFilename;
	
	public void run() throws Exception
	{
		init();
		randomKvector();
		
		int iterate = 0, error = Integer.MAX_VALUE;		
		
		while (iterate<iterateMax && error>endError)
		{
			KmeansJob job = new KmeansJob();
			job.inputPath = dataFilename;
			job.setKvector(m_kVectors, knum);
			job.outputPath = m_workspace + "result/" + (++iterate) + "/";			
			if (ToolRunner.run(job, null) != 0)
				break;
			
			error = calcError(job.outputPath);
			recordKvectors(m_workspace + iterate);
		}
		
		System.out.println("kmeans end " + iterate);
	}
	
	private void init() throws Exception
	{
		if (knum <= 1)
			throw new Exception("k error");
		
		if (dimension < 1 || dimension != max.length ||
			dimension != min.length)
			throw new Exception("dimension error");
		
		File f = new File(dataFilename);
		if (!f.exists() || !f.isFile())
			throw new Exception("input file not exists");
		
		Configuration conf = new Configuration();
		m_fs = FileSystem.get(conf);
		m_workspace = "/kmeans/";
		m_fs.mkdirs(new Path(m_workspace));
		
		m_resultFilename = new String[knum];
		for (Integer i = 0; i < knum; ++i)
			m_resultFilename[i] = "part-r-0000" + i.toString();
	}
	
	private void randomKvector() throws IOException
	{
		int[] diff = new int[dimension];
		for (int i = 0; i < dimension; ++i)
			diff[i] = max[i] - min[i] + 1;
		Random rand = new Random();
		
		m_kVectors = new ArrayList<Kvector>(knum);
		for (int i = 0; i < knum; ++i)
		{
			Kvector e = new Kvector(new int[dimension], i);
			for (int j = 0; j < dimension; ++j)
				e.array[j] = min[j] + rand.nextInt(diff[j]);
			m_kVectors.add(e);
		}
		
		recordKvectors(m_workspace + "0");
	}
	
	private int calcError(String dirPath) throws IOException
	{
		int error = 0;
		
		for (String s : m_resultFilename)
		{
			Path p = new Path(dirPath + s);
			FSDataInputStream in = m_fs.open(p);
			
			@SuppressWarnings("deprecation")
			String line = in.readLine();
			in.close();
			Kvector v_new = Kvector.fromString(line);
			
			Kvector v_last = m_kVectors.get(v_new.code);			
			int d = v_new.distance(v_last);
			if (d > error) error = d;
			m_kVectors.set(v_new.code, v_new);
		}

		return (int)Math.sqrt(error);
	}
	
	private void recordKvectors(String filename) throws IOException
	{
		StringBuilder str = new StringBuilder();
		for (Kvector e : m_kVectors)
			str.append(e.toString()).append("\n");
		
		FSDataOutputStream out = m_fs.create(new Path(filename));
		out.writeBytes(str.toString());
	}
}
