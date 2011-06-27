/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by Teresa Cottom, cottom1@llnl.gov CODE-400187 All rights reserved. This file is part of
 * RECONCILE
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License (as published by the Free Software Foundation) version 2, dated June 1991. This program is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full text see license.txt
 */
package reconcile.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class ZipInputFormat extends FileInputFormat<Text, Text>
{
	public static final String MAX_ENTRY_FILES = "InputFormat.maxEntryFilesToProcess";
	public static final String NUM_FILES_PER_SPLIT = "InputFormat.numberFilesPerSplit";
	public static final String IGNORE_FILES_LARGER_THAN_IN_MB = "InputFormat.ignoreFilesLargerThanInMB";
	public static final String PROCESS_MIME_TYPES_ONLY = "InputFormat.processMimeTypes";
	public static final int bufSize = 1024;

	static final Log LOG = LogFactory.getLog(ZipInputFormat.class);

	public static class ZipEntrySplit extends InputSplit implements Writable
	{	
	  	private Path file;
	  	private String entryPath;
		private long length;
	  	private JobContext context;
		
		public ZipEntrySplit() 
		{}
		
		public ZipEntrySplit(Path file, String entryPath, long length, JobContext context) 
		{
		    this.file = file;
		    this.entryPath = entryPath;
		    this.length = length;
		    this.context = context;
			LOG.info("ZipEntrySplit: file("+file.toString()+") entry("+entryPath+") length("+length+")");
		}
		
		  /** The zip archive containing this split's data. */
		  public Path getFile() {
		    return file;
		  }
		
		  /** The path of the file within the zip archive. */
		  public String getEntryPath() {
		    return entryPath;
		  }
		
		  public String getFullEntryPath() {
			  return file.toString()+"-"+entryPath;
		  }
		  
		  /** The uncompressed size of the file within the zip archive. */
		  @Override
		  public long getLength() throws IOException {
		    return length;
		  }

		  @Override
		  public void write(DataOutput out) throws IOException 
		  {
			  out.writeUTF(file.toString());
			  out.writeUTF(entryPath);
			  out.writeLong(length);
		  }
	
		  @Override
		  public void readFields(DataInput in) throws IOException 
		  {
			  file = new Path(in.readUTF());
			  entryPath = in.readUTF();
			  length = in.readLong();
			  LOG.info("ZipEntrySplit.readFields: file("+file.toString()+") entry("+entryPath+") length("+length+")");
		  }

		  @Override
		  public String[] getLocations() throws IOException 
		  {
			  TreeSet<String> hosts = new TreeSet<String>();
			  
			  FileSystem fs = FileSystem.get(context.getConfiguration());
			  FileStatus status = fs.getFileStatus(file);
			  for (BlockLocation block : fs.getFileBlockLocations(status, 0L, status.getLen())) {
				  for (String name : block.getHosts()) {
					  hosts.add(name);
				  }
			  }
			  return hosts.toArray(new String[hosts.size()]);
		  }
		
	}
	
	public static class MultiZipSplit extends InputSplit implements Writable
	{
		public ZipEntrySplit[] splits;
		
		public MultiZipSplit() { }
		public MultiZipSplit(Collection<ZipEntrySplit> splitList) {
			splits = splitList.toArray(new ZipEntrySplit[splitList.size()]);
		}
		
		@Override
		public long getLength() throws IOException {
			long sum = 0;
			for (ZipEntrySplit split : splits) {
				sum+=split.getLength();
			}
			return sum;
		}

		@Override
		public void write(DataOutput out) throws IOException 
		{
			out.writeInt(splits.length);
			for (ZipEntrySplit split : splits) {
				split.write(out);
			}
		}
	
		@Override
		public void readFields(DataInput in) throws IOException 
		{
			int num = in.readInt();
			splits = new ZipEntrySplit[num];
			for (int i=0; i<num; ++i) {
				splits[i] = new ZipEntrySplit();
				splits[i].readFields(in);
			}
		}

		@Override
		public String[] getLocations() throws IOException 
		{
			TreeSet<String> hosts = new TreeSet<String>();
			for (ZipEntrySplit split : splits) {
				for (String host : split.getLocations()) {
					hosts.add(host);
				}
			}
			return hosts.toArray(new String[hosts.size()]);
		}
	}
	
	private static class ProgressThread extends Thread 
	{
		private final TaskAttemptContext _reporter;
		private final long _reportIntervall;

		public ProgressThread(TaskAttemptContext context, long reportIntervall) {
			_reporter = context;
			_reportIntervall = reportIntervall;
			setDaemon(true);
		}
		@Override
		public void run()
		{
			try {
				while (true) {
					_reporter.progress();
					sleep(_reportIntervall);
				}
			}
			catch (final InterruptedException e) {
				LOG.debug("progress thread stopped");
			}
		}

		public static ProgressThread start(TaskAttemptContext reporter, long reportIntervall) {
			ProgressThread thread = new ProgressThread(reporter, reportIntervall);
			thread.start();
			return thread;
		}
	}

	public class ZipEntryRecordReader extends RecordReader<Text, Text>
	{
		FileSystem fs;
		MultiZipSplit split;	
		int index;
		ProgressThread thread;
		
		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException 
		{
			split = (MultiZipSplit) arg0;
			index = -1;
			fs = FileSystem.get(arg1.getConfiguration());
			thread = ProgressThread.start(arg1, 1000);
		}

		@Override
		public void close() throws IOException 
		{
			if (thread!=null) thread.interrupt();
		}

		@Override
		public Text getCurrentKey() 
			throws IOException, InterruptedException 
		{
			return new Text(split.splits[index].getFullEntryPath());
		}

		@Override
		public Text getCurrentValue() 
			throws IOException, InterruptedException 
		{
			byte[] value = loadZipEntry(fs, split.splits[index]);
			return new Text(value);
		}

		@Override
		public float getProgress() 
			throws IOException, InterruptedException 
		{
			return (1.0f * index) / split.splits.length;
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException 
		{
			++index;
			return index < split.splits.length;
		}		
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException
	{
		RecordReader<Text, Text> reader = new ZipEntryRecordReader();
		reader.initialize(arg0, arg1);
		
		return reader;
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) 
	{
		FileSystem fs;
		try {
			fs = FileSystem.get(context.getConfiguration());
		} 
		catch (IOException e) {
			e.printStackTrace();
			LOG.error("unable to connect to context based FileSystem to check if file is splitable");
			return false;
		}
		
	    LOG.debug("Verifying ZIP format for file: "+file.toString());
	    boolean splitable = true;
	    ZipInputStream zis = null;
	
	    try {
	    	zis = new ZipInputStream(fs.open(file));
	    	ZipEntry zipEntry = zis.getNextEntry();
	    	if (zipEntry == null) {
	    		throw new IOException("No entries found! Empty zip file: ");
	    	}
	    	LOG.debug("...ZIP format verification successful!");
	    } 
	    catch (IOException ioe) {
	    	LOG.error("Exception encountered while trying to open and read ZIP input stream: "+ioe.toString());
	    	splitable = false;
	    } 
	    finally {
	    	try {
	    		if (zis != null) {
	    			zis.close();
	    		}
	    	} 
	    	catch (IOException ioe) {
	    	  LOG.error("Exception while trying to close ZIP input stream: "+ioe);
	    	}
	    }
	    return splitable;
	}
	
	private FileNameMap fileNameMap = URLConnection.getFileNameMap();
	private List<ZipEntrySplit> getZipFileEntries(JobContext context, FileSystem fs, 
			Path[] zipFiles, Integer maxEntryFiles, Integer ignoreFilesLargerThanMB,
			List<String> processMimeTypes) 
		throws IOException
	{
	    ArrayList<ZipEntrySplit> splits = new ArrayList<ZipEntrySplit>();
	    ZipInputStream zis = null;
	    ZipEntry zipEntry = null;
	
	    for (int i = 0; i < zipFiles.length; i++) 
	    {
	      Path file = zipFiles[i];
	      LOG.debug("Opening zip file: "+file.toString());
	      try {
	    	  zis = new ZipInputStream(fs.open(file));
	    	  while ((zipEntry = zis.getNextEntry()) != null) 
	    	  {
	    		  if (maxEntryFiles!=null && splits.size()==maxEntryFiles.intValue()) {
	    			    LOG.debug("Exceeded maximum number of splits.  End getSplits()");
	    			    return splits;
	    		  }
	    		  
	    		  boolean processFile = true;
	    		  
	    		  if (processMimeTypes.size() > 0) 
	    		  {
	    			  // Ensure that if process mime types were specified, that entry
	    			  // mime type meets that criteria
	    			  String mimeType = fileNameMap.getContentTypeFor(zipEntry.getName());    		  
	    			  if (mimeType==null || (!processMimeTypes.contains(mimeType.toLowerCase()))) {
	    				  processFile = false;
	    				  LOG.debug("Ignoring entry file ("+zipEntry.getName()+" mimeType("+mimeType+") not in process list");
	    			  }
	    		  }
	    		  
	    		  long byteCount = zipEntry.getSize();
	    		  /*
	    		  if (byteCount <= 0) {
	    			  // Read entry and figure out size for ourselves
	    			  byteCount = 0;
	    			  while (zis.available()==1) {
	    				  zis.read();
	    				  ++byteCount;
	    			  }
	    		  }
	    		  */
	    		  if (ignoreFilesLargerThanMB!=null && byteCount > ignoreFilesLargerThanMB.intValue()) {
	    			  processFile = false;
	    			  LOG.debug("Ignoring entry file ("+zipEntry.getName()+") which exceeds size limit");
	    		  }

	    		  if (processFile) {
	    			  LOG.debug("Creating split for zip entry: "+zipEntry.getName()+
	    				  	" Size: "+byteCount+" Method: "+
	    				  	(ZipEntry.DEFLATED == zipEntry.getMethod() ? "DEFLATED" : "STORED")+
	    				  	" Compressed Size: "+zipEntry.getCompressedSize());	  
	    		  
	    			  ZipEntrySplit zipSplit = new ZipEntrySplit(file, zipEntry.getName(), zipEntry.getSize(), context);
	    			  splits.add(zipSplit);
	    		  }
	    		  zis.closeEntry();
	    	  }
	      } 
	      finally {
	    	  IOUtils.closeQuietly(zis);
	      }
	
	    }		
	    return splits;
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext context)
		throws IOException
	{		
		Integer maxEntryFiles = null;
		String value = context.getConfiguration().get(MAX_ENTRY_FILES);
		if (value!=null) {
			maxEntryFiles = Integer.parseInt(value);
			LOG.info("Maximum number of zip entries:"+maxEntryFiles);
		}

		value = context.getConfiguration().get(NUM_FILES_PER_SPLIT, "1");
		int numFilesPerSplit = Integer.parseInt(value);
		LOG.info("Number of files per split:"+numFilesPerSplit);
		
		Integer ignoreFilesLargerThanMB=null;
		value = context.getConfiguration().get(IGNORE_FILES_LARGER_THAN_IN_MB);
		if (value!=null) {
			ignoreFilesLargerThanMB = Integer.parseInt(value);
			LOG.info("Ignore entry files larger than ("+ignoreFilesLargerThanMB+")MB in size");
		}
		
		List<String> processMimeTypes = new ArrayList<String>();
		value = context.getConfiguration().get(PROCESS_MIME_TYPES_ONLY);
		if (value!=null) {
			for (String val : value.split(",")) {
				if (val!=null && (val.length()!=0)) {
					val = val.toLowerCase();
					processMimeTypes.add(val);
					LOG.info("Process entry files with mime type ("+val+")");
				}
			}
		}
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
	    LOG.debug("Start splitting input ZIP files");
	    
	    Path[] files = getInputPaths(context);
	    for (int i = 0; i < files.length; i++) { // check we have valid files
	    	Path file = files[i];
	    	FileStatus status = fs.getFileStatus(file);
	    	if (!fs.exists(file)) {
	    		throw new IOException("Input file provided ("+files[i]+") does not exist.");
	    	}
	    	if (status.isDir()) {
	    		throw new IOException("Input file provided ("+files[i]+") is not a file but a directory.");
	    	}
	    }
	
	    //  Get all entry zip splits
	    List<ZipEntrySplit> splits = getZipFileEntries(context, fs, files, maxEntryFiles, ignoreFilesLargerThanMB, processMimeTypes);
	    LOG.info("There are ("+splits.size()+") zip entry splits");
	    
	    // Determine final number of combined zip entry splits
	    ArrayList<InputSplit> finalSplits = new ArrayList<InputSplit>();
	    int totalSplits = splits.size() / numFilesPerSplit;
	    if ((splits.size() % numFilesPerSplit)!= 0) 
	    	++totalSplits;	    
	    LOG.info("There will be ("+totalSplits+") MultiZipSplit(s)");

		// Group zip entry splits into MultiZipSplits 
	    int begin=0, end=0;
	    for (int i=0; i<totalSplits; ++i)
	    {
	    	end = begin + numFilesPerSplit; 
	    	
	    	if ((i+1)==totalSplits)
	    		end = splits.size();
	    	
		    LOG.info("\t MultiZipSplit begin("+begin+") end("+end+")");
	    	MultiZipSplit split = new MultiZipSplit(splits.subList(begin, end));
	    	finalSplits.add(split);

	    	begin = end;
	    }
	    
	    LOG.debug("End splitting input ZIP files.");
	    return finalSplits;
	  }
	
	private static byte[] read(String entry, ZipInputStream zis, int numBytes)
	{
		byte[] data = new byte[numBytes];
		int n=0;
	    try {
			while (zis.available()==1 && (n<numBytes)) {
				data[n] = (byte) zis.read();
				++n;
			}
		} 
	    catch (IOException e) {
	    	LOG.error("failure reading zip entry("+entry+")");
			e.printStackTrace();
			return null;
		}
		LOG.info("Read bytes("+n+") from entry ("+entry+")");
		LOG.debug("Read value("+Bytes.toString(data)+") from entry ("+entry+")");
		
		return data;
	}

	private static byte[] read(String entry, ZipInputStream zis)
	{
		ArrayList<byte[]> dataArray = new ArrayList<byte[]>();
		byte[] current = null;
		
		int i=0;
		int n=0;
	    try {
			while (zis.available()==1) {
				if (n%bufSize == 0) {
					current = new byte[bufSize];
					dataArray.add(current);
					i=0;
				}
				current[i] = (byte) zis.read();
				++n; ++i;
			}
		} 
	    catch (IOException e) {
	    	LOG.error("failure reading zip entry("+entry+")");
			e.printStackTrace();
			return null;
		}
	    --n;

	    // Copy multiple buffers into single large buffer
	    byte[] data = new byte[n];
	    i=0;
	    for (byte[] buffer : dataArray) {
	    	int copyLength = bufSize;
	    	if ( (i+copyLength) > n) {
	    		copyLength = n - i;
	    	}
	    	for (int j=0; j<copyLength; ++j) {
	    		data[i] = buffer[j];
	    		++i;
	    	}
	    }
	    
		LOG.info("Read bytes("+n+") from entry ("+entry+")");
		LOG.debug("Read value("+Bytes.toString(data)+") from entry ("+entry+")");
				
		return data;
	}

	public static byte[] loadZipEntry(FileSystem fs, ZipEntrySplit zipSplit)
		throws IOException
	{
		byte[] data = null;
		Path file = zipSplit.getFile();
		String entryPath = zipSplit.getEntryPath();
		
		ZipInputStream zis=null;	
		try {
			zis = new ZipInputStream(fs.open(file));
			ZipEntry zipEntry = zis.getNextEntry();
			while (zipEntry != null && !zipEntry.getName().equals(entryPath)) {
				zipEntry = zis.getNextEntry();
			}
			if (zipEntry!=null) {
				if (zipSplit.getLength() > 0) {
					data = read(zipSplit.getEntryPath(), zis, (int)zipSplit.getLength());
				}
				else {
					data = read(zipSplit.getEntryPath(), zis);
				}
			}
		}
		finally {
			IOUtils.closeQuietly(zis);
		}				
		return data;
	}

}

