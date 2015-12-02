package Main;

import Kmeans.Kmeans;

public class Main
{
	public static void main(String[] args) throws Exception
	{
		Kmeans k = new Kmeans();
		k.knum = 3;
		k.dimension = 3;
		k.min = new int[] { 90, 60, 155 };
		k.max = new int[] { 130, 100, 195 };
		k.dataFilename = "data";
		k.iterateMax = 4;
		k.run();
	}
}