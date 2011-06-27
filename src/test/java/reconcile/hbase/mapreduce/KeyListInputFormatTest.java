package reconcile.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mortbay.log.Log;

import reconcile.hbase.mapreduce.KeyListInputFormat.KeyListSplit;
import reconcile.hbase.table.DocSchemaTest;

public class KeyListInputFormatTest extends TestCase
{
	public static final String testDir = "/test/KeyListInputFormatTests";
	public static final Path testPath = new Path(testDir);

	public ArrayList<String> inputKeys =  new ArrayList<String>();

    private KeyListInputFormat inputFormat = new KeyListInputFormat();
    private Job job;
	private Configuration conf;

	@Override
	public void setUp()
		throws Exception
	{
		conf = HBaseConfiguration.create();
		job = new Job(conf, "Squirrel");
		job.getConfiguration().set(JobConfig.TABLE_CONF, DocSchemaTest.tableName);
		inputKeys.clear();
	}

	@Override
	public void tearDown()
	{
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(testPath, true);
		}
		catch (IOException e) {
			Log.warn("Error occurred during ("+getClass().getSimpleName()+".tearDown");
			e.printStackTrace();
		}
	}

	public void testSimpleSplit() throws Exception
	{
		// Create key list file
		inputKeys.add(DocSchemaTest.key);
		Path keyFile = new Path(testPath, "input_keys.txt");
		createKeysFile(keyFile, inputKeys);

		// Set input path
		addInputPath(keyFile);

		// Get splits
	    JobContext context = new JobContext(job.getConfiguration(), new JobID("FakeID", 1));
	    List<InputSplit> splits = inputFormat.getSplits(context);

	    // Validate
	    Assert.assertEquals(1, splits.size());
	    KeyListSplit split = (KeyListSplit) splits.get(0);
	    Assert.assertEquals(DocSchemaTest.tableName, split.getTableName());
	    Assert.assertEquals(1, split.getKeys().size());
	    Assert.assertEquals(DocSchemaTest.key, split.getKeys().get(0));
	}

	public void testMultipleSplit() throws Exception
	{
		// Create key list files
		final int numDuplicated = 5;
		final int numFiles = 3;
		final int numKeys = Integer.parseInt(KeyListInputFormat.DEFAULT_SPLIT_SIZE) + 2;
		final int totalKeys = numKeys * numFiles;
		int k=0;
		for (int j=0; j<numFiles; ++j)
		{
			Path keyFile = new Path(testPath, "input_keys_"+j+".txt");
			for (int i=0; i<numKeys; ++i, ++k) {
				inputKeys.add("key-"+k);
			}
			createKeysFile(keyFile, inputKeys);
			inputKeys.clear();
			addInputPath(keyFile);
		}
		Path keyFile = new Path(testPath, "input_keys_duplicated.txt");
		for (int i=0; i<numDuplicated; ++i) {
			inputKeys.add("key-"+i);
		}
		createKeysFile(keyFile, inputKeys);
		addInputPath(keyFile);

		// Get splits
	    JobContext context = new JobContext(job.getConfiguration(), new JobID("FakeID", 1));
	    List<InputSplit> splits = inputFormat.getSplits(context);

	    // Validate correct number of splits
	    Assert.assertEquals("incorrect number of splits", numFiles+1, splits.size());

	    // Count keys and track number of times each key occurs
	    TreeMap<String, Integer> count = new TreeMap<String, Integer>();
	    int numTotalKeys = 0;
	    for (InputSplit isplit : splits) {
		    KeyListSplit split = (KeyListSplit) isplit;
		    Assert.assertEquals(DocSchemaTest.tableName, split.getTableName());
		    for (String key : split.getKeys()) {
		    	++numTotalKeys;
		    	if (count.containsKey(key)) {
		    		count.put(key, count.get(key).intValue()+1);
		    	}
		    	else {
        count.put(key, 1);
		    	}
		    }
	    }
	    // Validate correct number of overall keys
	    Assert.assertEquals("incorrect number of total keys", totalKeys, numTotalKeys);

	    // Validate no duplicate keys!
	    for (String key : count.keySet()) {
	    	Assert.assertEquals("key ("+key+") found more than once", 1, (int)count.get(key));
	    }
	}

	private void addInputPath(Path path) throws IOException
	{
		FileInputFormat.addInputPath(job, path);
	}

    public static void createKeysFile(Path file, List<String> keys)
    	throws IOException
    {
    	Configuration conf = HBaseConfiguration.create();
    	FileSystem fs = FileSystem.get(conf);
    	FSDataOutputStream os = null;
    	try {
            os = fs.create(file, true);
            for (String key : keys) {
              os.writeBytes(key+"\n");
            }
            os.flush();
    	}
    	finally {
            if (os!=null) {
            	os.close();
            }
    	}
    }

}
