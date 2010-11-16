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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CountQualifiers
    extends Configured
    implements Tool {

private static final Log LOG = LogFactory.getLog(CountQualifiers.class);

private static final String TABLE_NAME = "query.table_name";

private static final String COLUMN_FAMILY = "query.column_family";

private static final String COLUMN_QUALIFIER = "query.column_qualifier";

private String table = "example";

private String columnFamily;

private String columnQualifer;

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>table
 *          <li>column family
 *          <li>column qualifier
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new CountQualifiers(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}


public int run(String[] args)
    throws Exception
{
  if (args.length != 3) {
    System.out.println("usage: CountRows <table name> <column name> <column qualifier>");
    return 1;
  }
  HBaseConfiguration conf = new HBaseConfiguration();

  try {
    table = args[0];
    columnFamily = args[1];
    columnQualifer = args[2];
    conf.set(TABLE_NAME, table);
    Job job = null;
    LOG.info("Before map/reduce startup");
    job = new Job(conf, "query: count cols");
    job.setJarByClass(CountQualifiers.class);
    job.setNumReduceTasks(1);
    job.getConfiguration().set(TABLE_NAME, table);
    job.getConfiguration().set(COLUMN_FAMILY, columnFamily);
    job.getConfiguration().set(COLUMN_QUALIFIER, columnQualifer);

    Scan scan = new Scan();
    scan.addColumn(columnFamily.getBytes(), columnQualifer.getBytes());

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

@Override
public void setup(Context context)
{
  try {
    super.setup(context);

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

  context.getCounter("CountRows", "row count").increment(1);
}


}
}
