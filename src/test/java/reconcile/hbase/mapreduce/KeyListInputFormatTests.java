package reconcile.hbase.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import reconcile.hbase.query.FiltersTests;

import reconcile.hbase.table.DocSchema;

import junit.framework.Assert;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;

public class KeyListInputFormatTests extends TestCase 
{
	public static final String testPath = "/test/KeyListInputFormatTests";
	Path testFile;
	Path outputFile;
	Path junkFile;
	
	@Override
	public void setUp()
	{
		HBaseConfiguration conf = new HBaseConfiguration();
		try {
			FileSystem fs = FileSystem.get(conf);
			Path dir = new Path(testPath);
			fs.mkdirs(dir);
			testFile = new Path(dir, "input.txt");
			junkFile = new Path(dir, "other.txt");
			outputFile = new Path(dir, "output.txt");
			fs.delete(junkFile, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void tearDown()
	{
		HBaseConfiguration conf = new HBaseConfiguration();
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(junkFile, true);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void testJob() throws Exception
	{
		FSDataInputStream is=null;
		BufferedReader reader = null;
		
		FiltersTests.createKeysFile(testFile);
		
		String[] args = { testFile.toString(), junkFile.toString() };
		ToolRunner.run(new Configuration(), new TestJob(), args);

		// Validate contents of output file
		HBaseConfiguration conf = new HBaseConfiguration();
		FileSystem fs = FileSystem.get(conf);
		
		boolean gtFound=false, pubmedFound=false;
		int numLines = 0;
		try {
			is = fs.open(outputFile);
			reader = new BufferedReader(new InputStreamReader(is));
		
			String line=null;
			while ((line=reader.readLine())!=null) {
				if (line.startsWith(FiltersTests.GT_DOC_KEY)) {
					Assert.assertTrue(line.endsWith(":GT2009"));
					gtFound = true;
				}
				else if (line.startsWith(FiltersTests.PUBMED_DOC_KEY)) {
					Assert.assertTrue(line.endsWith(":PubMed"));
					pubmedFound = true;
				}
				++numLines;
			}
		}
		finally {
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(is);
		}
		Assert.assertEquals(2, numLines);  // should only be two entries
		Assert.assertTrue(gtFound);
		Assert.assertTrue(pubmedFound);
	}
	
	public static void main(String[] args)
	{
		junit.framework.TestSuite suite = new TestSuite();
		suite.addTestSuite(KeyListInputFormatTests.class);

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
			job.setJarByClass(KeyListInputFormatTests.class);
			
			job.setMapperClass(ValidateMapper.class);
			job.setInputFormatClass(KeyListInputFormat.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Result.class);

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

public static class ValidateMapper extends Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Result> {

	
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
public void map(ImmutableBytesWritable mapKey, Result result, Context context)
    throws IOException, InterruptedException
{
	context.getCounter(getClass().getSimpleName(), "map").increment(1L);
	context.write(mapKey, result);
}

}

public static class ValidateReducer extends TableReducer<ImmutableBytesWritable, Result, ImmutableBytesWritable> 
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
	  Path path = new Path(KeyListInputFormatTests.testPath+"/output.txt");
	  fs.delete(path, false);
	  os = fs.create(path, true);
	  writer = new OutputStreamWriter(os);
	}

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context)
	    throws IOException, InterruptedException
	{
		context.getCounter(getClass().getSimpleName(), "reduce").increment(1);
	  
		StringBuffer buffer = new StringBuffer();
		
		for (Result result : values) {			
			String source = DocSchema.getColumn(result, DocSchema.srcCF, DocSchema.srcName);
			if (source!=null) {
				context.getCounter(getClass().getSimpleName(), "source").increment(1);
				buffer.append(Bytes.toString(key.get())+":"+source+"\n");	  
			}
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
