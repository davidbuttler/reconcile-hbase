/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by David Buttler, buttler1@llnl.gov CODE-400187 All rights reserved. This file is part of
 * RECONCILE
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License (as published by the Free Software Foundation) version 2, dated June 1991. This program is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full text see license.txt
 */
package reconcile.hbase.query.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Map;
import java.util.NavigableMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ScanQualifiers
    extends Configured
    implements Tool {

private static final Log LOG = LogFactory.getLog(ScanQualifiers.class);

private static final String TABLE_NAME = "query.table_name";

//private static final String COLUMN_FAMILY = "query.column_family";

private String table = "example";


/**
 *Count the unique tab separated qualifier values
 *
 * @param args
 *          :
 *          <ol>
 *          <li>table name
 *          <li>column family
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new ScanQualifiers(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

private ArrayList<String> columnFamilies = new ArrayList<String>();

public int run(String[] args)
    throws Exception
{
  if (args.length < 2) {
    System.out.println("usage: ScanQualifiers <table name> <column name>...<column name 2> ");

    return 1;
  }
  Configuration conf = HBaseConfiguration.create();

  try {
    table = args[0];
    
    for (int i=1; i<args.length; ++i) {
    	columnFamilies.add(args[i]);
    }
    Job job = null;
    LOG.info("Before map/reduce startup");
    job = new Job(conf, "query: count qualifier values ("+Arrays.toString(args)+")");
    job.setJarByClass(ScanQualifiers.class);
    job.setNumReduceTasks(1);
    job.getConfiguration().set(TABLE_NAME, table);
    //job.getConfiguration().set(COLUMN_FAMILY, columnFamily);

    Scan scan = new Scan();
    for (String columnFamily : columnFamilies) {
    	scan.addFamily(columnFamily.getBytes());
    }
    TableMapReduceUtil.initTableMapperJob(table, scan, CountRowMapper.class, ImmutableBytesWritable.class,
        Put.class, job);
    TableMapReduceUtil.initTableReducerJob(table, IdentityTableReducer.class, job);
    job.setNumReduceTasks(0);

    LOG.info("Started " + table);
    job.waitForCompletion(true);
    LOG.info("After map/reduce completion");

  }
  catch (Exception e) {
    e.printStackTrace();
    return 1;
  }

  return 0;
}

public static class CountRowMapper
    extends TableMapper<ImmutableBytesWritable, Put> {

//private String columnFamily;


public static final Pattern pTab = Pattern.compile("\\t");

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
    //columnFamily = context.getConfiguration().get(COLUMN_FAMILY);
  }
  catch (IOException e) {
    e.printStackTrace();
  }
  catch (InterruptedException e) {
    e.printStackTrace();
  }

}

/**
 * Pass the key, value to reduce.
 *
 * @param key
 *          The current key.
 * @param value
 *          The current value.
 * @param context
 *          The current context.
 * @throws IOException
 *           When writing the record fails.
 * @throws InterruptedException
 *           When the job is aborted.
 */
@Override
public void map(ImmutableBytesWritable key, Result row, Context context)
    throws IOException, InterruptedException
{
	NavigableMap<byte[], NavigableMap<byte[],byte[]>> map = row.getNoVersionMap();
	for (byte[] colFamily : map.keySet())
	{
		String family = Bytes.toString(colFamily);
		NavigableMap<byte[], byte[]> qualMap = map.get(colFamily);
		for (byte[] colQual : qualMap.keySet()) 
		{	
			String qualifier = Bytes.toString(colQual);
			context.getCounter("CountVals", family+":"+qualifier).increment(1);
		}
	}
	//Map<byte[], byte[]> map = row.getFamilyMap(columnFamily.getBytes());
	//for (byte[] k : map.keySet()) {
	//  String v = new String(k, HConstants.UTF8_ENCODING);
	//  context.getCounter("CountVals", v).increment(1);
	// }
}


}
}
