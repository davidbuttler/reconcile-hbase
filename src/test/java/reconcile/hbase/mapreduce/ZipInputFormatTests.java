package reconcile.hbase.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.FileArchiver;
import util.ResourceFile;

import junit.framework.Assert;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;

/**
 * To run test via command-line:
 *    ant hadoopTest -Dinput=reconcile.hbase.mapreduce.ZipInputFormatTests
 *    
 * @author cottom1
 *
 */
public class ZipInputFormatTests extends TestCase 
{
	public static final String testPath = "/test/ZipInputFormatTests";
	public static final String fileOne = "file-one.txt";
	public static final String fileTwo = "file-two.txt";
	public static final String zipFile = "file.zip";
	
	public static final String jobOutputFile = "file-out.txt";
	public static final String jobJunkFile = "file-junk.txt";
	
	public static final String fileOneText = "This is the text contained within file one.";
	public static final String fileTwoText = "Text in file two, ok????";
	
	public static String getFullPath(String name) {
		return ResourceFile.HDFS_PREFIX+getHDFSPath(name);
	}
	
	public static String getHDFSPath(String name) {
		return testPath + "/" + name;
	}
	
	private void writeFile(String name, String text) throws IOException
	{
		OutputStream os=null;
		BufferedWriter writer = null;
		try {
			os = ResourceFile.getOutputStream(getFullPath(name));
			writer = new BufferedWriter(new OutputStreamWriter(os));
			writer.write(text);
		}
		finally {
			IOUtils.closeQuietly(writer);
			IOUtils.closeQuietly(os);
		}		
	}
	
	@Override
	public void setUp()
		throws IOException
	{
		HBaseConfiguration conf = new HBaseConfiguration();
		FileSystem fs = FileSystem.get(conf);

		Path dir = new Path(testPath);
		fs.mkdirs(dir);
			
		fs.delete(new Path(getHDFSPath(zipFile)), true);
		fs.delete(new Path(getHDFSPath(jobOutputFile)), false);
		fs.delete(new Path(getHDFSPath(jobJunkFile)), true);
		
		writeFile(fileOne, fileOneText);
		writeFile(fileTwo, fileTwoText);

		String[] args = { "-output="+getFullPath(zipFile), "-input="+getFullPath(fileOne), "-input="+getFullPath(fileTwo), "-gzipFiles=false" };
		FileArchiver.main(args);
		
		System.out.println("Zip file is created!  Setup is complete");
	}
	
	@Override
	public void tearDown()
	{}
	
	public void testJob() throws Exception
	{
		FSDataInputStream is=null;
		BufferedReader reader = null;
		
		String[] args = { getHDFSPath(zipFile), getHDFSPath(jobJunkFile) };
		ToolRunner.run(new Configuration(), new TestJob(), args);

		// Validate contents of output file
		HBaseConfiguration conf = new HBaseConfiguration();
		FileSystem fs = FileSystem.get(conf);
		
		System.out.println("Output file:");
		int count=0;
		boolean fileOneFound=false, fileTwoFound=false;
		try {
			is = fs.open(new Path(getHDFSPath(jobOutputFile)));
			reader = new BufferedReader(new InputStreamReader(is));
		
			String line=null;
			while ((line=reader.readLine())!=null) {
				++count;
				if (line.equals(fileOne+":"+fileOneText)) {
					fileOneFound = true;
				}
				else if (line.equals(fileTwo+":"+fileTwoText)) {
					fileTwoFound = true;
				}
			}
		}
		finally {
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(is);
		}
		Assert.assertEquals(2, count);
		Assert.assertTrue(fileOneFound);
		Assert.assertTrue(fileTwoFound);
	}
	
	public static void main(String[] args)
	{
		junit.framework.TestSuite suite = new TestSuite();
		suite.addTestSuite(ZipInputFormatTests.class);

		TestResult result = new TestResult();
		suite.run(result);
		
		System.out.println("Tests run:"+result.runCount()+" Errors:"+result.errorCount()+" Failed:"+result.failureCount());
	}

public static class TestJob extends Configured implements Tool {

	private HBaseConfiguration conf;

	public TestJob() 
	{}

	public int run(String[] args)	
	{
		String inputPath = args[0];
		String outputPath = args[1];
		conf = new HBaseConfiguration();
		conf.set("mapred.map.tasks.speculative.execution", "false");

		try {

			Job job = null;
			job = new Job(conf, getClass().getSimpleName());
			job.setJarByClass(ZipInputFormatTests.class);
			
			job.setMapperClass(ValidateMapper.class);
			job.setInputFormatClass(ZipInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setReducerClass(ValidateReducer.class);
		    job.setNumReduceTasks(1);	    

			job.waitForCompletion(true);
		}
		catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}
}

public static class ValidateMapper extends Mapper<Text, Text, Text, Text> {

	
@Override
public void setup(Context context)
{
	try {
		super.setup(context);
    
		context.getCounter(getClass().getSimpleName(), "setup").increment(1L);
	}
	catch (FileNotFoundException e) {
		e.printStackTrace();
	}
	catch (IOException e) {
		e.printStackTrace();
	}
	catch (InterruptedException e) {
		e.printStackTrace();
	}
}

@Override
protected void cleanup(Context context1)
    throws IOException, InterruptedException
{}

@Override
public void map(Text mapKey, Text result, Context context)
    throws IOException, InterruptedException
{
	context.getCounter(getClass().getSimpleName(), "map").increment(1L);
	context.write(mapKey, result);
}

}

public static class ValidateReducer extends TableReducer<Text, Text, Text> 
{
	FSDataOutputStream os;
	OutputStreamWriter writer;
	
	@Override
	protected void setup(Context context)
	    throws IOException, InterruptedException
	{
	  context.getCounter(getClass().getSimpleName(), "setup").increment(1);
	  
	  Configuration conf = context.getConfiguration();
	  FileSystem fs = FileSystem.get(conf);
	  Path path = new Path(getHDFSPath(jobOutputFile));
	  fs.delete(path, false);
	  os = fs.create(path, true);
	  writer = new OutputStreamWriter(os);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException
	{
		context.getCounter(getClass().getSimpleName(), "reduce").increment(1);
	  
		StringBuffer buffer = new StringBuffer();
		
		for (Text result : values) {			
			context.getCounter(getClass().getSimpleName(), result.toString()).increment(1);
			buffer.append(key.toString()+":"+result.toString()+"\n");	  
		}		
		writer.write(buffer.toString());
	}

	@Override
	protected void cleanup(Context context1)
	    throws IOException, InterruptedException
	{
		IOUtils.closeQuietly(writer);
		IOUtils.closeQuietly(os);
	}

}

}
