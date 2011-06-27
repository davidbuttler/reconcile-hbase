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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
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

import com.google.common.base.Charsets;

public class DeleteLargeColumnFamily
    extends Configured
    implements Tool {

private static final Log LOG = LogFactory.getLog(DeleteLargeColumnFamily.class);

private static final String TABLE_NAME = "query.table_name";
private static final String COLUMN_FAMILY = "query.columnFamily";


/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>table name
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new DeleteLargeColumnFamily(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

public int run(String[] args)
    throws Exception
{
  if (args.length != 2) {
    System.out.println("usage: SizeColumnFamily <table name> <column family>");
    return 1;
  }
  Configuration conf = HBaseConfiguration.create();

  try {
    String tableName = args[0];
    String columnFamily = args[1];
    conf.set(TABLE_NAME, tableName);
    conf.set(COLUMN_FAMILY, columnFamily);
    Job job = null;
    LOG.info("Before map/reduce startup");
    job = new Job(conf, "query: size CF");
    job.setJarByClass(DeleteLargeColumnFamily.class);
    job.setNumReduceTasks(1);
    job.getConfiguration().set(TABLE_NAME, args[0]);

    Scan scan = new Scan();
    scan.addFamily(columnFamily.getBytes());

    TableMapReduceUtil.initTableMapperJob(tableName, scan, CountRowMapper.class, ImmutableBytesWritable.class,
        Put.class, job);
    TableMapReduceUtil.initTableReducerJob(tableName, IdentityTableReducer.class, job);
    job.setNumReduceTasks(0);

    LOG.info("Started " + tableName);
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

String columnFamily;

private HTable table;
private static final int ONE_K = 1024;

private static final int ONE_M = ONE_K * ONE_K;

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
    columnFamily = context.getConfiguration().get(COLUMN_FAMILY);

    String tableName = context.getConfiguration().get(TABLE_NAME);
    Configuration conf = HBaseConfiguration.create();
    table = new HTable(conf, tableName.getBytes());
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
 * @param row
 *          The current row.
 * @param context
 *          The current context.
 * @throws IOException
 *           When writing the record fails.
 * @throws InterruptedException
 *           When the job is aborted.
 */
@Override
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
public void map(ImmutableBytesWritable key, Result row, Context context)
    throws IOException, InterruptedException
{
  Map<byte[], byte[]> cf = row.getFamilyMap(columnFamily.getBytes());
  for (byte[] col: cf.keySet()) {
    String qual = new String(col, Charsets.UTF_8);
    byte[] val = cf.get(col);
    int size = val.length;
    if (size / ONE_M > 0) {
      Delete delete = new Delete(row.getRow());
      table.delete(delete);
      context.getCounter("delete", String.valueOf(size / ONE_M)).increment(1);
      break;
    }
    else {
      SizeRows.outputRowSize(context, qual, size);
    }
  }

}


@Override
protected void cleanup(Context context1)
    throws IOException, InterruptedException
{
  super.cleanup(context1);
  while (true) {
    try {
      table.flushCommits();
      table.close();
    }
    catch (IOException e) {
      context1.getCounter("Base Reconcile Annotation error", "io exception in flush/close").increment(1);
      e.printStackTrace();
    }
    break;
  }
  System.out.println("done");
}

}
}
