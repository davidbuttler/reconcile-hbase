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
	public static boolean startedCorrectly = false;

	private static final String saveMimeType = "text/plain";
	
	public static final String testPath = "/test/ZipInputFormatTests";
	public static final String fileOne = "file-one.txt";
	public static final String fileTwo = "file-two.txt";
	public static final String fileThree = "file-three.txt";
	public static final String fileFour = "file-four.textorama";
	public static final String zipFile = "file.zip";
	
	public static final String jobOutputFile = "file-out.txt";
	public static final String jobJunkFile = "file-junk.txt";
	
	public static final String fileOneText = "This is the text contained within file one.";
	public static final String fileTwoText = "Text in file two, ok????";
	public static final String fileFourText = "Attention file four, attention file four";
	
	private static String textThree = null;
	public static String fileThreeText() {
		if (textThree==null) {
			int length = ZipInputFormat.bufSize*3+10;
			StringBuffer buffer = new StringBuffer();
			char val = 'a';
			for (int i=0; i<length; ++i) {
				buffer.append(val);
				++val;
				if (!Character.isLowerCase(val)) {
					val = 'a';
				}
			}
			textThree = buffer.toString();
		}
		return textThree;
	}
	
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
		if (!startedCorrectly) return;

		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(conf);

		Path dir = new Path(testPath);
		fs.mkdirs(dir);
			
		fs.delete(new Path(getHDFSPath(zipFile)), true);
		fs.delete(new Path(getHDFSPath(jobOutputFile)), false);
		fs.delete(new Path(getHDFSPath(jobJunkFile)), true);
		
		writeFile(fileOne, fileOneText);
		writeFile(fileTwo, fileTwoText);
		writeFile(fileThree, fileThreeText());
		writeFile(fileFour, fileFourText);

		String[] args = { "-output="+getFullPath(zipFile), 
				"-input="+getFullPath(fileOne), 
				"-input="+getFullPath(fileTwo), 
				"-input="+getFullPath(fileThree), 
				"-input="+getFullPath(fileFour), 
				"-gzipFiles=false" };
		
		FileArchiver.main(args);
		
		System.out.println("Zip file is created!  Setup is complete");
	}
	
	@Override
	
	
	public void tearDown()
	{
		if (!startedCorrectly) return;
	}
	
	public void testJob() throws Exception
	{
		if (!startedCorrectly) {
			System.out.println("Skipping "+getClass().getSimpleName()+".testJob() as this was not started under hadoop");
			return;
		}
		
		FSDataInputStream is=null;
		BufferedReader reader = null;
		
		String[] args = { getHDFSPath(zipFile), getHDFSPath(jobJunkFile) };
		ToolRunner.run(new Configuration(), new TestZipInputFormatJob(), args);

		// Validate contents of output file
		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(conf);
		
		System.out.println("Output file("+jobOutputFile+"):");
		int count=0;
		boolean fileOneFound=false, fileTwoFound=false, fileThreeFound=false, fileFourFound=false;
		try {
			is = fs.open(new Path(getHDFSPath(jobOutputFile)));
			reader = new BufferedReader(new InputStreamReader(is));
		
			String line=null;
			while ((line=reader.readLine())!=null) {
				++count;
				if (line.endsWith(fileOne+":"+fileOneText)) {
					fileOneFound = true;
				}
				else if (line.endsWith(fileTwo+":"+fileTwoText)) {
					fileTwoFound = true;
				}
				else if (line.endsWith(fileThree+":"+fileThreeText())) {
					fileThreeFound = true;
				}
				else if (line.contains(fileFour+":")) {
					fileFourFound = true;
				}
			}
		}
		finally {
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(is);
		}
		Assert.assertEquals(3, count);
		Assert.assertTrue(fileOneFound);
		Assert.assertTrue(fileTwoFound);
		Assert.assertTrue(fileThreeFound);
		Assert.assertFalse(fileFourFound);
	}
	
	public static void main(String[] args)
	{
		startedCorrectly = true;
		
		junit.framework.TestSuite suite = new TestSuite();
		suite.addTestSuite(ZipInputFormatTests.class);

		TestResult result = new TestResult();
		suite.run(result);
		
		System.out.println("Tests run:"+result.runCount()+" Errors:"+result.errorCount()+" Failed:"+result.failureCount());
	}

public static class TestZipInputFormatJob extends Configured implements Tool {

	private Configuration conf;

	public TestZipInputFormatJob() 
	{}

	public int run(String[] args)	
	{
		String inputPath = args[0];
		String outputPath = args[1];
		conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set(ZipInputFormat.PROCESS_MIME_TYPES_ONLY, saveMimeType);

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
