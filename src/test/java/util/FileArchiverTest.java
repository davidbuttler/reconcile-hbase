package util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.emory.mathcs.backport.java.util.Arrays;

public class FileArchiverTest extends TestCase {

	private static final String hdfsDir = "/test/FileArchiverTest";
	private static final Path hdfsPath = new Path(hdfsDir);

	private static final String localDir = "/tmp/FileArchiverTest";
	private static final Path localPath = new Path(localDir);

	private static final String subDir = "subdirectory";
	private static final String file1 = "one.txt";
	private static final String file2 = "two.xml";
	private static final String file3 = "three.txt";
	private static final String file4 = "four.xml";

	private static final String contents1 = "This is text file one.";
	private static final String contents2 = "<two>This is xml file two.</two>";
	private static final String contents3 = "Text file three has this for contents.";
	private static final String contents4 = "<four>Whatever ok</four>";


	private Configuration conf;
	private FileSystem localFS, hdfs;
	private FileArchiver archiver = new FileArchiver();
	private ArrayList<String> inputNames = new ArrayList<String>();
	private String outputName;
	private String hdfsOutputName;

	public FileArchiverTest() throws IOException
	{
		conf = new Configuration();
		localFS = FileSystem.getLocal(conf);
		hdfs = FileSystem.get(conf);
	}

	@Override
	public void setUp()
	{
		cleanPaths();

		try {
			createFiles(localFS, localPath);
			createFiles(hdfs, hdfsPath);

			outputName = this.getName()+".zip";
			hdfsOutputName = ResourceFile.HDFS_PREFIX+"/test/"+outputName;
		}
		catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void tearDown()
	{
		cleanPaths();
	}

	private void cleanPaths()
	{
		outputName = this.getName()+".zip";
		hdfsOutputName = ResourceFile.HDFS_PREFIX+"/test/"+outputName;
		try {
			localFS.delete(new Path(localDir), true);
			hdfs.delete(new Path(hdfsDir), true);
			localFS.delete(new Path(outputName), true);
			hdfs.delete(new Path(hdfsOutputName), true);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createFiles(FileSystem fs, Path dirPath) throws IOException
	{
		fs.mkdirs(dirPath);
		Path subPath = new Path(dirPath, subDir);
		fs.mkdirs(subPath);

		createFile(fs, dirPath, file1, contents1);
		createFile(fs, dirPath, file2, contents2);
		createFile(fs, subPath, file3, contents3);
		createFile(fs, subPath, file4, contents4);
	}

	public static void createFile(FileSystem fs, Path dirPath, String fileName, String contents)
		throws IOException
	{
		Path file = new Path(dirPath, fileName);
		OutputStream os = null;
		BufferedWriter writer = null;
		try {
			os = fs.create(file, true);
			writer = new BufferedWriter(new OutputStreamWriter(os));
			writer.write(contents);
		}
		catch (IOException e) {
			throw e;
		}
		finally {
			IOUtils.closeQuietly(writer);
			IOUtils.closeQuietly(os);
		}
	}

	public void test_estimateZipSize() throws IOException
	{
		inputNames.add(localDir+"/*");
		double totalSize = archiver.archive("foo.zip", inputNames, false, false, true);
		Assert.assertTrue(Math.abs(totalSize-54) < 1.0e-10);
	}

	public void test_LocalZip_LocalFilePattern_NoGZip_NoRecursive() throws IOException
	{
		String[] expected = { file1 };

		inputNames.add(localDir+"/*.txt");
		archiver.archive(outputName, inputNames, false, false, false);

		checkContentsOfLocalZipFile(outputName, expected);
	}

	public void test_LocalZip_HDFSFilePattern_NoGZip_NoRecursive() throws IOException
	{
		String[] expected = { file1 };

		inputNames.add(ResourceFile.HDFS_PREFIX+hdfsDir+"/*.txt");
		archiver.archive(outputName, inputNames, false, false, false);

		checkContentsOfLocalZipFile(outputName, expected);
	}

	public void test_LocalZip_LocalDir_NoGZip_Recursive() throws IOException
	{
		String[] expected = { file1, file2, subDir+"/"+file3, subDir+"/"+file4 };

		inputNames.add(localDir);
		archiver.archive(outputName, inputNames, false, true, false);

		checkContentsOfLocalZipFile(outputName, expected);
	}

	public void test_LocalZip_HDFSDir_NoGZip_Recursive() throws IOException
	{
		String[] expected = { file1, file2, subDir+"/"+file3, subDir+"/"+file4 };

		inputNames.add(ResourceFile.HDFS_PREFIX+hdfsDir);
		archiver.archive(outputName, inputNames, false, true, false);

		checkContentsOfLocalZipFile(outputName, expected);
	}

	public void test_LocalZip_LocalFilePattern_GZip_NoRecursive() throws IOException
	{
		String[] expected = { file1+".gz" };

		inputNames.add(localDir+"/*.txt");
		archiver.archive(outputName, inputNames, true, false, false);

		checkContentsOfLocalZipFile(outputName, expected);
	}

	public void test_LocalZip_HDFSFilePattern_GZip_NoRecursive() throws IOException
	{
		String[] expected = { file1+".gz" };

		inputNames.add(ResourceFile.HDFS_PREFIX+hdfsDir+"/*.txt");
		archiver.archive(outputName, inputNames, true, false, false);

		checkContentsOfLocalZipFile(outputName, expected);

		// Extraction test [from=local to=HDFS]
		String outputDir = ResourceFile.HDFS_PREFIX+hdfsDir + "/contents";
		extract_and_verify(outputName, outputDir, hdfs, expected);
}

	public void test_HDFSZip_HDFSFilePattern_GZip_NoRecursive() throws IOException
	{
		String[] expected = { file1+".gz" };

		inputNames.add(ResourceFile.HDFS_PREFIX+hdfsDir+"/*.txt");
		archiver.archive(hdfsOutputName, inputNames, true, false, false);

		checkContentsofHDFSZipFile(expected);

		// Extraction test [from=HDFS to=HDFS]
		String outputDir = ResourceFile.HDFS_PREFIX+hdfsDir + "/contents";
		extract_and_verify(hdfsOutputName, outputDir, hdfs, expected);
	}

	private void extract_and_verify(String zipFile, String outputDir, FileSystem outFS, String[] expected)
		throws IOException
	{
		archiver.extract(zipFile, outputDir);

		// Verify
		Path outputPath = new Path(outputDir);
		Assert.assertTrue("extracted HDFS output directory ("+outputPath.getName()+") does not exist", outFS.exists(outputPath));
		for (String file : expected) {
			Path path = new Path(outputPath, file);
			Assert.assertTrue("extracted file ("+path.getName()+") does not exist in HDFS", outFS.exists(path));
		}
	}

	public void checkContentsofHDFSZipFile(String[] expected) throws IOException
	{
		hdfs.copyToLocalFile(new Path(hdfsOutputName), new Path(outputName));
		checkContentsOfLocalZipFile(outputName, expected);
	}

    public static void checkContentsOfLocalZipFile(String zipFileName, String[] expected)
    	throws IOException
    {
    	ArrayList<String> entries = new ArrayList<String>();
    	ZipFile zipFile = new ZipFile(zipFileName);

    	Enumeration<?> zipEntries = zipFile.entries();
    	while (zipEntries.hasMoreElements()) {
    		entries.add(((ZipEntry)zipEntries.nextElement()).getName());
    	}
    	String[] values = entries.toArray(new String[entries.size()]);

  Assert.assertEquals("number of zip entries found(" + entries.size() + ") does not match number expected("
      + expected.length + ")...entries=" + Arrays.toString(values) + " expected=" + Arrays.toString(expected),
    			expected.length, entries.size());
    	for (String expectedEntry : expected) {
    		Assert.assertTrue("zip entry does not contain expected entry ("+expectedEntry+")",
    				entries.contains(expectedEntry));
    	}
    }

}
