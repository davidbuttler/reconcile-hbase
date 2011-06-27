package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import junit.framework.Assert;
import junit.framework.TestCase;

public class FileListGeneratorTest extends TestCase 
{
	private static final String TEST_DIR = "/test/FileListGeneratorTest";
	private static final String LIST_FILE = TEST_DIR+"/listFile.txt";
	private static final String IN_FILE_1 = "foo";
	private static final String IN_FILE_2 = "bar";

	private Configuration conf;
	private FileSystem fs;
	
	@Override
	public void setUp()
	{
		try {
			super.setUp();
			conf = new Configuration();		
			fs = FileSystem.get(conf);
			Path testPath = new Path(TEST_DIR);
			fs.mkdirs(testPath);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void tearDown() throws Exception
	{
		fs.delete(new Path(LIST_FILE), false);
		super.tearDown();
	}
	
	public void test() throws IOException
	{
		String[] args = { 
				FileListGenerator.INPUT_FILE_ARG+IN_FILE_1,
				FileListGenerator.INPUT_FILE_ARG+IN_FILE_2,
				FileListGenerator.LIST_FILE_ARG+LIST_FILE
		};
		FileListGenerator.process(args);
		Assert.assertTrue(fs.exists(new Path(LIST_FILE)));
		
		InputStream is = null;
		BufferedReader reader = null;
		try {
			is = fs.open(new Path(LIST_FILE));
			reader = new BufferedReader(new InputStreamReader(is));
			Assert.assertEquals(IN_FILE_1, reader.readLine());
			Assert.assertEquals(IN_FILE_2, reader.readLine());
		}
		finally {
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(is);
		}
	}
	
	public void testNoListFile() throws Exception
	{
		String[] args = { 
				FileListGenerator.INPUT_FILE_ARG+IN_FILE_1,
				FileListGenerator.INPUT_FILE_ARG+IN_FILE_2,
		};
		try {
			FileListGenerator.process(args);
		}
		catch (Exception e) {
			if (!e.getMessage().contains(FileListGenerator.LIST_FILE_ARG)) {
				throw e;
			}
			else {
				return;
			}
		}
		Assert.fail("FileListGenerator should have thrown exception for missing list file but did not");
	}
	
	public void testNoInputFiles() throws IOException
	{
		String[] args = { 
				FileListGenerator.LIST_FILE_ARG+LIST_FILE
		};
		
		FileListGenerator.process(args);
		Assert.assertFalse(fs.exists(new Path(LIST_FILE)));
	}
}
