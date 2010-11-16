package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;

public class ResourceFile {

	public static final String HDFS_PREFIX="hdfs:";
	
	public static boolean isGzipFile(String fileName)
	{
		return fileName.endsWith(".gz");
	}
	
	public static boolean isHDFSFile(String fileName)
	{
		return fileName.startsWith(HDFS_PREFIX);
	}
	
	public static String hdfsFileName(String fileURI)
	{
		return fileURI.substring(HDFS_PREFIX.length());
	}
	
	public static InputStream getInputStream(String fileURI)
		throws IOException
	{
		if (isHDFSFile(fileURI)) {
			// Meta data file is a file within HDFS
			Configuration conf = new Configuration();
			DFSClient dfsClient = new DFSClient(conf);
			String hdfsFileName = hdfsFileName(fileURI);
			return dfsClient.open(hdfsFileName);
		}
		else  {
			File metaFile = new File(fileURI);
			if (metaFile.exists()) {
				// Meta data file is a file on local file system
				return new FileInputStream(metaFile);
			}
			else {
				// Try meta data file as resource available in jar
				try {
					throw new IOException("foo");
				}
				catch (IOException e) {
					Class<?> callerClass = e.getStackTrace()[1].getClass();
					return callerClass.getResourceAsStream(fileURI);		
				}
			}
		}
	}
	
	public static OutputStream getOutputStream(String fileURI) 
		throws IOException
	{
		OutputStream os = null;
		
		if (isHDFSFile(fileURI)) {
			// Meta data file is a file within HDFS
			Configuration conf = new Configuration();
			DFSClient dfsClient = new DFSClient(conf);
			String hdfsFileName = hdfsFileName(fileURI);
			
			// Create file in HDFS
			os = dfsClient.create(hdfsFileName, true);
		}
		else {
			// Create file on the local file system
			File metaFile = new File(fileURI);
			os = new FileOutputStream(metaFile);
		}
		// Create a GZIP File if 'requested'
		if (isGzipFile(fileURI)) {
			os = new GZIPOutputStream(os);
		}
		return os;
	}

}