package util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileArchiver {

	public static final Log LOG = LogFactory.getLog(FileArchiver.class);
	
	
	private Configuration conf;
	private FileSystem fs;
	
	public FileArchiver() 
		throws IOException 
	{
		conf = new Configuration();
		fs = FileSystem.get(conf);
	}
	
	private void copyFile(InputStream is, OutputStream os) 
		throws IOException
	{
		for (int c = is.read(); c != -1; c = is.read()) {
			os.write(c);
		}		
	}
	
	private Path gzipFile(Path hdfsPath) 
		throws IOException
	{
		String outName = hdfsPath.getName()+".gz";
		Path hdfsPathOut = new Path(hdfsPath.getParent(), outName);
		LOG.info("....compressing file ("+hdfsPath+") to file("+hdfsPathOut+")");
		
		// Gzip the file before adding to archive
		InputStream is = null;  OutputStream os = null;
		try {
			is = fs.open(hdfsPath);
			os = new GZIPOutputStream(fs.create(hdfsPathOut, true));
			
			copyFile(is, os);
		}
		finally {
			IOUtils.closeQuietly(is);
			IOUtils.closeQuietly(os);
		}
		return hdfsPathOut;
	}
	
	private void addFileToZip(ZipOutputStream os, Path hdfsPath, boolean gzipFiles) 
		throws IOException
	{	
		// Gzip input file first
		Path hdfsPathOut = hdfsPath;
		if (gzipFiles)
			hdfsPathOut = gzipFile(hdfsPath);
						
		// Determine zip entry name
		String entryName = hdfsPathOut.getName();
		LOG.info("....creating archive entry ("+entryName+")");
		ZipEntry entry = new ZipEntry(entryName);
		
		FileStatus status = fs.getFileStatus(hdfsPathOut);
		System.out.println("Setting size of ("+hdfsPathOut+") to ("+status.getLen()+")");
		entry.setSize(status.getLen());
		
		// Create new entry in archive
		os.putNextEntry(entry);
		
		// Copy compressed version of file into entry
		LOG.info("....adding data from file ("+hdfsPathOut+")");
		InputStream is = null;
		try {
			is = fs.open(hdfsPathOut);
			copyFile(is, os);
		}
		finally {
			IOUtils.closeQuietly(is);
		}
		LOG.info("....closing archive entry");
		// Close entry
		os.closeEntry();		
	}
	
	public void archive(String outputURI, Collection<String> inputPaths)
		throws IOException
	{
		archive(outputURI, inputPaths, true);
	}
	
	public void archive(String outputURI, Collection<String> inputPaths, boolean gzipFiles)
		throws IOException
	{
		ZipOutputStream os=null;
		
		try {
			LOG.info("Creating archive file ("+outputURI+")");
			if (ResourceFile.isHDFSFile(outputURI)) {
				String hdfsFileName = ResourceFile.hdfsFileName(outputURI);
				os = new ZipOutputStream(fs.create(new Path(hdfsFileName)));
			}
			else {
				os = new ZipOutputStream(ResourceFile.getOutputStream(outputURI));				
			}
		
			for (String pathName : inputPaths)
			{
				if (ResourceFile.isHDFSFile(pathName)) 
				{
					
					String hdfsPath = ResourceFile.hdfsFileName(pathName);
					Path path = new Path(hdfsPath);
					
					for (FileStatus status : fs.globStatus(path)) {
						addFileToZip(os, status.getPath(), gzipFiles);
					}
				}
        else
          throw new IOException("currently does not support archiving with local file system input");
			}
		}
		finally {
			IOUtils.closeQuietly(os);
		}
		LOG.info("closing archive file");
	}
	
	public static final String OUTPUT_ARG = "-output=";
	public static final String INPUT_ARG = "-input=";
	public static final String GZIP_ARG = "-gzipFiles=";
	
	public static void main(String[] args) 
		throws IOException
	{
		boolean gzipFiles = true;
		String outputFile = null;
		ArrayList<String> inputPaths = new ArrayList<String>();
		
		for (String arg : args)
		{
			if (arg.startsWith(OUTPUT_ARG)) {
				outputFile = arg.substring(OUTPUT_ARG.length());
			}
			else if (arg.startsWith(INPUT_ARG)) {
				String inputPath = arg.substring(INPUT_ARG.length());
				for (String path : inputPath.split(",")) {
					inputPaths.add(path);
				}
			}
			else if (arg.startsWith(GZIP_ARG)) {
				gzipFiles = Boolean.parseBoolean(arg.substring(GZIP_ARG.length()));
			}			
		}
		FileArchiver archiver = new FileArchiver();
		archiver.archive(outputFile, inputPaths, gzipFiles);
	}
}
