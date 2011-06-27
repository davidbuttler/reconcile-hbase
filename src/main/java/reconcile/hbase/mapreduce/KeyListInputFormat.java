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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import reconcile.hbase.table.DocSchema;

public class KeyListInputFormat extends FileInputFormat<ImmutableBytesWritable, Result>
{
static final Log LOG = LogFactory.getLog(KeyListInputFormat.class);

static final String TOP = "reconcile.hbase.mapreduce.KeyListInputFormat";
static final public String SCAN_FAMILIES = TOP+".scanFamilies";
static final public String LOG_ON = TOP+".logOn";
static final String SPLIT_SIZE=TOP+".keyListSplitSize";
static final String MAX_SPLITS=TOP+".maxSplits";

static final String DEFAULT_SPLIT_SIZE = "200";
static final String DEFAULT_MAX_SPLITS = "100";

/**
 * KeyListSplit - Split portion of a KeyListInputFormat
 *
 * @author cottom1
 */
public static class KeyListSplit extends InputSplit implements Writable
{
	private List<String> keys = new ArrayList<String>();
	private String[] hosts;
	private String tableName;

	public KeyListSplit()
	{}

	public KeyListSplit(String tableName, List<String> keys, String[] hosts)
	{
		this.tableName = tableName;
		this.keys.addAll(keys);
		this.hosts = hosts;
		LOG.info("KeyListSplit: num keys("+this.keys.size()+") table("+tableName+") num hosts("+hosts.length+")");
	}

	public List<String> getKeys() {
		return keys;
	}

	public String getTableName()
	{
		return tableName;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(tableName);
		out.writeInt(keys.size());
		for (String key : keys) {
			out.writeUTF(key);
		}
		out.writeInt(hosts.length);
		for (String host : hosts) {
			out.writeUTF(host);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		tableName = in.readUTF();
		int nitems = in.readInt();
		for (int i=0; i<nitems; ++i) {
			keys.add(in.readUTF());
		}
		nitems = in.readInt();
		hosts = new String[nitems];
		for (int i=0; i<nitems; ++i) {
			hosts[i] = in.readUTF();
		}
		LOG.info("KeyListSplit: read num keys("+keys.size()+") num hosts("+hosts.length+") table("+tableName+")");
	}

  @Override
  public String[] getLocations() throws IOException
  {
	  return hosts;
  }

  @Override
  public long getLength()
  	throws IOException, InterruptedException
  {
	  return keys.size();
  }

}

/**
 * KeyRowReader - Works off of a KeyListSplit, and loops over keys in that split
 * retrieving the records for pre-selected columns from HBase
 *
 * @author cottom1
 *
 */
private static class KeyRowReader extends RecordReader<ImmutableBytesWritable, Result>
{
    private Result result;
    private ImmutableBytesWritable key;

    private DocSchema table;
    private boolean logOn = false;
    private TreeSet<String> scanFamilies = new TreeSet<String>();
    private KeyListSplit split;
    private int ndx;

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
		throws IOException, InterruptedException
	{
		ndx = 0;
		split = (KeyListSplit) arg0;
		table = new DocSchema(split.tableName);

		// Set the families retrieved by scan
		String value = arg1.getConfiguration().get(SCAN_FAMILIES);
		if (value!=null) {
			for (String family : StringUtils.split(value)) {
				scanFamilies.add(family);
			}
		}

		// Enable / disable logging
		value = arg1.getConfiguration().get(LOG_ON);
		if (value!=null) {
			logOn = Boolean.parseBoolean(value);
		}
	}

    @Override
    public ImmutableBytesWritable getCurrentKey()
    {
    	return key;
    }

    @Override
    public Result getCurrentValue()
    {
    	return result;
    }

    @Override
    public boolean nextKeyValue()
    	throws IOException
    {
    	result = null;
    	key = null;

    	while ( (ndx<split.getKeys().size()) && result==null)
    	{
    		String stringKey = split.getKeys().get(ndx);
    		if (logOn) {
    			LOG.info("Search for record with key:"+stringKey);
    		}

    		byte[] byteKey = Bytes.toBytes(stringKey);
    		key = new ImmutableBytesWritable(byteKey);
    		Get get = new Get(byteKey);

    		// Add scan families
    		for (String family : scanFamilies) {
    			if (logOn) {
    				LOG.info("... add get family("+family+")");
    			}
    			get.addFamily(family.getBytes());
    		}

    		result = table.get(get);

    		if (logOn) {
    			if (result==null) {
    				LOG.info("Unable to find record:"+stringKey);
    			}
    			else {
    				LOG.info("Loaded record:"+stringKey);
    			}
    		}
    		++ndx;
    	}
    	return result!=null;
    }

    @Override
    public float getProgress()
    {
    	if (ndx==0 || split==null) return (float) 0.0;
  return ndx / (float) split.getKeys().size();
    }

	@Override
	public void close() throws IOException {
	}
}


@Override
public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
		InputSplit arg0, TaskAttemptContext arg1) throws IOException,
		InterruptedException
{
	KeyRowReader reader = new KeyRowReader();
	reader.initialize(arg0, arg1);
	return reader;
}

@Override
protected boolean isSplitable(JobContext context, Path filename)
{
	return true;
}


public static String[] getLocations(HConnection connection, String tableName, List<String> keys) throws IOException
{
	  TreeSet<String> hosts = new TreeSet<String>();

	  for (String key : keys) {
		  HRegionLocation region = connection.getRegionLocation(tableName.getBytes(), key.getBytes(), true);
		  String hostName = region.getServerAddress().getHostname();
		  LOG.debug("Key("+key+") found on host ("+hostName+")");
		  hosts.add(hostName);
	  }
	  return hosts.toArray(new String[hosts.size()]);
}

// private HConnection getHConnection() throws ZooKeeperConnectionException
// {
//
// Configuration conf = HBaseConfiguration.create();
// return HConnectionManager.getConnection(conf);
// }

@SuppressWarnings("deprecation")
@Override
public List<InputSplit> getSplits(JobContext context)
	throws IOException
{
	ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
	TreeSet<String> uniqueKeys = new TreeSet<String>();

	FileSystem fs = FileSystem.get(context.getConfiguration());

	final String tableName = context.getConfiguration().get(JobConfig.TABLE_CONF);
	if (tableName==null || tableName.length()==0) throw new IOException("HBase table name was not provided");
	int splitSize = Integer.parseInt(context.getConfiguration().get(SPLIT_SIZE, DEFAULT_SPLIT_SIZE));

	final int maxSplits = Integer.parseInt(context.getConfiguration().get(MAX_SPLITS, DEFAULT_MAX_SPLITS));

    Path[] files = getInputPaths(context);
	for (Path file : files)
	{
    	if (fs.isDirectory(file) || !fs.exists(file)) throw new IOException("Not a valid key list file: "+file.toString());

    	InputStream is = null;
    	BufferedReader reader = null;
    	try {
    		is = fs.open(file);
    		reader = new BufferedReader(new InputStreamReader(is));
    		String line = null;
    		while ((line=reader.readLine())!=null) {
    			String key = line.trim();
    			if (key.length() > 0 && !key.startsWith("#")) {
            uniqueKeys.add(key);
          }
    		}
    	}
    	finally {
    		IOUtils.closeQuietly(is);
    		IOUtils.closeQuietly(reader);
    	}
	}

	ArrayList<String> keys = new ArrayList<String>();
	keys.addAll(uniqueKeys);

    if (keys.size() > 0)
    {
    	HConnection conn = HConnectionManager.getConnection(context.getConfiguration());

    	int numSplits = Math.max((int)Math.ceil(keys.size()/(splitSize*1.0)), 1);
    	if (numSplits > maxSplits) {
    		splitSize = (int)Math.ceil(keys.size() / (maxSplits*1.0));
    		LOG.info("Overriding split size with ("+splitSize+") to maintain max splits of ("+maxSplits+")");
        	numSplits = Math.max((int)Math.ceil(keys.size()/(splitSize*1.0)), 1);
    	}

    	LOG.info("There are ("+keys.size()+") total keys. Split size("+splitSize+")  Num splits("+numSplits+")");
    	int startNdx = 0;
    	for (int i=0; i<(numSplits-1); ++i) {
    		int endNdx = startNdx + splitSize;
    		splits.add(createSplit(conn, tableName, keys, startNdx, endNdx));
    		startNdx = endNdx;
    	}
    	// Add last split
    	splits.add(createSplit(conn, tableName, keys, startNdx, keys.size()));
    }
	return splits;
}

private KeyListSplit createSplit(HConnection connection, String tableName, List<String> keys, int startNdx, int endNdx)
	throws IOException
{
	List<String> splitKeys = keys.subList(startNdx, endNdx);

	String[] hosts = getLocations(connection, tableName, splitKeys);

	return new KeyListSplit(tableName, splitKeys, hosts);
}

}
