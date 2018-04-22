import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AprioriMapReduce {

	static List<List<String>> currentCandidateSets = new ArrayList<List<String>>();

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			File checker = new File("/home/cloudera/MROP/part-r-00000");
			StringTokenizer itr = new StringTokenizer(value.toString());
			if (!checker.exists()) {
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken());
					context.write(word, one);
				}
			} else {
				String[] CurrentInputLine = value.toString().split(" ");
				List<String> CurrentInputLineList = Arrays
						.asList(CurrentInputLine);
				for (List<String> list : currentCandidateSets) {
					if (CurrentInputLineList.containsAll(list)) {
						String out = "";
						for (String string : list) {
							out = out + string + " ";
						}
						out = out.trim();
						word.set(out);
						context.write(word, one);
					}
				}
			}

		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum > 100) {
				result.set(sum);
				context.write(key, result);
			}
		}
	}

	public List<String> getdistinctElements(String input) {
		List<String> inputList = new ArrayList<String>();
		return inputList;
	}

	public static String getcsvString(List<String> input) {
		String result = "";
		for (String string : input) {
			result = result + string + ",";
		}

		if (result.length() > 0) {
			result = result.substring(0, result.length() - 1);
		}
		return result;
	}

	public static List<String> getFrequents() {
		List<String> frequentPairs = new ArrayList<String>();
		FileReader fileReader = null;
		BufferedReader brReader = null;
		try {
			fileReader = new FileReader("/home/cloudera/MROP/part-r-00000");
			brReader = new BufferedReader(fileReader);
			String line = null;
			// ["1 2","3 4"...]
			while ((line = brReader.readLine()) != null) {
				String[] parts = line.split("\\t");
				if (parts.length > 0)
					frequentPairs.add(parts[0]);
			}
		} catch (Exception e) {

		} finally {
			try {
				brReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return frequentPairs;
	}

	public static void copyFileToLocal(Configuration conf, String outputPath)
			throws IOException {
		org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem
				.get(conf);

		Path hdfs = new Path(outputPath + "/part-r-00000");
		Path local = new Path("/home/cloudera/MROP/part-r-00000");
		if (fs.exists(hdfs)) {
			fs.copyToLocalFile(false, hdfs, local, true);
		}
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		String inputPath = args[0];
		File input = new File(inputPath);
		int maxBasKetLength = findMaxbasketLength(inputPath);
		List<String> tempResult = new ArrayList<String>();
		String outputPath = input.getParent() + "/Output";
		File checker = new File("/home/cloudera/MROP/part-r-00000");
		File FrFile = new File("/home/cloudera/MROP/FrequentsItems.txt");

		try {
			if (checker.exists())
				checker.delete();
			if (FrFile.exists())
				FrFile.delete();
		} catch (Exception e) {

		}
		Configuration conf = new Configuration();
		int counter = 0;
		List<String> result = new ArrayList<String>();
		while (true) {
			Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(WordCountAllAtOnce.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			Path outPath = new Path(outputPath);
			FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
			if (dfs.exists(outPath)) {
				dfs.delete(outPath, true);
			}

			if (job.waitForCompletion(true)) {
				// System.out.println("Done" + counter);
			}
			copyFileToLocal(conf, outputPath);

			try {
				if (getFrequents().size() == 0)
					break;
				else {
					tempResult = new ArrayList<String>();
					tempResult = getFrequents();
					result.addAll(tempResult);
				}
			} catch (Exception e) {
			}
			counter = counter + 1;
			writeFrequents(tempResult, counter);
			System.out.print(counter + " ");
			System.out.print(tempResult.size());
			System.out.println();
			if (counter + 1 <= maxBasKetLength) {
//				currentCandidateSets = generateProcessedCandidateSetsType1(counter + 1);
				currentCandidateSets = generateProcessedCandidateSets();
			} else {
				System.out.println("Exceeded Max basket length!");
				break;
			}
		}
		System.out.println("The frequents! Count is - " + result.size());
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
	}

	private static void writeFrequents(List<String> tempResult, int phase) {
		File frequentItemsFile = new File(
				"/home/cloudera/MROP/FrequentsItems.txt");
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(frequentItemsFile, true));
			writer.write("Phase -" + phase);
			writer.write("\n");
			for (String string : tempResult) {
				writer.write("[" + string + "]" + ",");
				writer.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static int findMaxbasketLength(String filename) {
		File frequentItemsFile = new File(filename);
		FileReader fileReader = null;
		BufferedReader brReader = null;
		int max = 0;
		try {
			fileReader = new FileReader(filename);
			brReader = new BufferedReader(fileReader);
			String line = null;
			while ((line = brReader.readLine()) != null) {
				String[] parts = line.split(" ");
				if (parts.length > max)
					max = parts.length;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				brReader.close();
				fileReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return max;
	}

	private static List<List<String>> generateProcessedCandidateSetsType1(int a) {
		// TODO Auto-generated method stub
		String mapperFrequents = "";
		mapperFrequents = getcsvString(getFrequents());
		List<String> singletons = cartesian.fetchSingleton(mapperFrequents); // 1,2,3
		List<String> currentLevelCandidates = Arrays.asList(mapperFrequents // [[1
																			// //
																			// 2],[2
																			// 3]...
				.split(","));
		List<List<String>> tempElements = new ArrayList<List<String>>();
		tempElements.add(singletons);
		tempElements.add(currentLevelCandidates);
		tempElements = cartesian.getCandidateSetsType1(singletons, a);
		return tempElements;
	}
	
	private static List<List<String>> generateProcessedCandidateSets() {
		// TODO Auto-generated method stub
		String mapperFrequents = "";
		mapperFrequents = getcsvString(getFrequents());
		List<String> singletons = cartesian.fetchSingleton(mapperFrequents); // 1,2,3
		List<String> currentLevelCandidates = Arrays.asList(mapperFrequents // [[1
																			// //
																			// 2],[2
																			// 3]...
				.split(","));
		List<List<String>> tempElements = new ArrayList<List<String>>();
		tempElements.add(singletons);
		tempElements.add(currentLevelCandidates);
		tempElements = cartesian.getCandidateSets(tempElements);
		return tempElements;
	}
}
