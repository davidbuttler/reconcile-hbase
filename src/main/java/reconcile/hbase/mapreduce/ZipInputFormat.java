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
import java.util.ArrayList;
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
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ZipInputFormat extends FileInputFormat<Text, Text>
{
	public static final String MAX_SPLITS = "ZipInputFormat.maxDocuments";
	
	static final Log LOG = LogFactory.getLog(ZipInputFormat.class);

	public static class ZipSplit extends InputSplit implements Writable
	{	
	  	private Path file;
	  	private String entryPath;
		private long length;
	  	private JobContext context;
		
		public ZipSplit() 
		{}
		
		public ZipSplit(Path file, String entryPath, long length, JobContext context) 
		{
		    this.file = file;
		    this.entryPath = entryPath;
		    this.length = length;
		    this.context = context;
			LOG.info("ZipSplit: file("+file.toString()+") entry("+entryPath+") length("+length+")");
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
		
		  // //////////////////////////////////////////
		  // Writable methods
		  // //////////////////////////////////////////
		  public void write(DataOutput out) throws IOException {
			  UTF8.writeString(out, file.toString());
			  UTF8.writeString(out, entryPath);
			  out.writeLong(length);
		  }
	
		  public void readFields(DataInput in) throws IOException 
		  {
			  file = new Path(UTF8.readString(in));
			  entryPath = UTF8.readString(in);
			  length = in.readLong();
			  LOG.info("ZipSplit.readFields: file("+file.toString()+") entry("+entryPath+") length("+length+")");
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
	
	public class ZipEntryRecordReader extends RecordReader<Text, Text>
	{
		FileSystem fs;
		ZipSplit zipSplit;		
		byte[] currentValue;
		
		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException 
		{
			zipSplit = (ZipSplit) arg0;
			currentValue = null;
			fs = FileSystem.get(arg1.getConfiguration());
		}

		@Override
		public void close() throws IOException 
		{}

		@Override
		public Text getCurrentKey() 
			throws IOException, InterruptedException 
		{
			return new Text(zipSplit.getFullEntryPath());
		}

		@Override
		public Text getCurrentValue() 
			throws IOException, InterruptedException 
		{
			return new Text(currentValue);
		}

		@Override
		public float getProgress() 
			throws IOException, InterruptedException 
		{
			if (currentValue==null) return 0.0f;
			return 1.0f;
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException 
		{
			if (currentValue != null) return false;
			
			currentValue = loadZipEntry(fs, zipSplit);
			return true;
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
	
	@Override
	public List<InputSplit> getSplits(JobContext context)
		throws IOException
	{		
		Integer maxSplits = null;
		String value = context.getConfiguration().get(MAX_SPLITS);
		if (value!=null) {
			maxSplits = Integer.parseInt(value);
			LOG.info("Maximum number of splits set to:"+maxSplits);
		}
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
	    LOG.debug("Start splitting input ZIP files");
	    
	    Path[] files = getInputPaths(context);
	    for (int i = 0; i < files.length; i++) { // check we have valid files
	    	Path file = files[i];
	    	if (fs.isDirectory(file) || !fs.exists(file)) {
	    		throw new IOException("Not a file: "+files[i]);
	    	}
	    }
	
	    // generate splits
	    ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
	    ZipInputStream zis = null;
	    ZipEntry zipEntry = null;
	
	    for (int i = 0; i < files.length; i++) {
	      Path file = files[i];
	      LOG.debug("Opening zip file: "+file.toString());
	      try {
	    	  zis = new ZipInputStream(fs.open(file));
	    	  while ((zipEntry = zis.getNextEntry()) != null) 
	    	  {
	    		  if (maxSplits!=null && splits.size()==maxSplits.intValue()) {
	    			    LOG.debug("Exceeded maximum number of splits.  End getSplits()");
	    			    return splits;	    			  
	    		  }
	    		  
	    		  long byteCount = zipEntry.getSize();
	    		  if (byteCount <= 0) {
	    			  // Read entry and figure out size for ourselves
	    			  byteCount = 0;
	    			  while (zis.available()==1) {
	    				  zis.read();
	    				  ++byteCount;
	    			  }
	    		  }

	    		  LOG.debug("Creating split for zip entry: "+zipEntry.getName()+
	    				  	" Size: "+zipEntry.getSize()+" Method: "+
	    				  	(ZipEntry.DEFLATED == zipEntry.getMethod() ? "DEFLATED" : "STORED")+
	    				  	" Compressed Size: "+zipEntry.getCompressedSize());	  
	    		  
	    		  
	    		  ZipSplit zipSplit = new ZipSplit(file, zipEntry.getName(), zipEntry.getSize(), context);
	    		  splits.add(zipSplit);
	    		  
	    		  zis.closeEntry();
	    	  }
	      } 
	      finally {
	    	  IOUtils.closeQuietly(zis);
	      }
	
	    }
	    LOG.debug("End splitting input ZIP files.");
	    return splits;
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
	
	public static byte[] loadZipEntry(FileSystem fs, ZipSplit zipSplit)
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
				data = read(zipSplit.getEntryPath(), zis, (int)zipSplit.getLength());
			}
		}
		finally {
			IOUtils.closeQuietly(zis);
		}				
		return data;
	}

}

