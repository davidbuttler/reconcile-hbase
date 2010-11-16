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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SizeRows
    extends Configured
    implements Tool {

private static final Log LOG = LogFactory.getLog(SizeRows.class);

private static final String TABLE_NAME = "query.table_name";


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
    ToolRunner.run(new Configuration(), new SizeRows(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}


public int run(String[] args)
    throws Exception
{
  if (args.length != 1) {
    System.out.println("usage: SizeRows <table name>");
    return 1;
  }
  HBaseConfiguration conf = new HBaseConfiguration();

  try {
    String tableName = args[0];
    conf.set(TABLE_NAME, tableName);
    Job job = null;
    LOG.info("Before map/reduce startup");
    job = new Job(conf, "query: size rows");
    job.setJarByClass(SizeRows.class);
    job.setNumReduceTasks(1);
    job.getConfiguration().set(TABLE_NAME, args[0]);

    Scan scan = new Scan();

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

@SuppressWarnings({ "rawtypes" })
public static void outputRowSize(Context context, String name, int size)
{
  if (size / CountRowMapper.ONE_K == 0) {
    context.getCounter(name, "1 K").increment(1);
  }
  else if (size / (10 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "10 K").increment(1);
  }
  else if (size / (100 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "100 K").increment(1);
  }
  else if (size / (200 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "200 K").increment(1);
  }
  else if (size / (300 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "300 K").increment(1);
  }
  else if (size / (400 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "400 K").increment(1);
  }
  else if (size / (500 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "500 K").increment(1);
  }
  else if (size / (600 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "600 K").increment(1);
  }
  else if (size / (700 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "700 K").increment(1);
  }
  else if (size / (800 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "800 K").increment(1);
  }
  else if (size / (900 * CountRowMapper.ONE_K) == 0) {
    context.getCounter(name, "900 K").increment(1);
  }
  else if (size / (CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "1 M").increment(1);
  }
  else if (size / (2 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "2 M").increment(1);
  }
  else if (size / (3 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "3 M").increment(1);
  }
  else if (size / (4 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "4 M").increment(1);
  }
  else if (size / (5 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "5 M").increment(1);
  }
  else if (size / (6 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "6 M").increment(1);
  }
  else if (size / (7 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "7 M").increment(1);
  }
  else if (size / (8 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "8 M").increment(1);
  }
  else if (size / (9 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "9 M").increment(1);
  }
  else if (size / (10 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "10 M").increment(1);
  }
  else if (size / (100 * CountRowMapper.ONE_M) == 0) {
    context.getCounter(name, "100 M").increment(1);
  }
  else {
    context.getCounter(name, "BIG").increment(1);
  }
}

public static class CountRowMapper
    extends TableMapper<ImmutableBytesWritable, Put> {

private static final int ONE_K = 1024;

private static final int ONE_M = ONE_K * ONE_K;

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
public void map(ImmutableBytesWritable key, Result row, Context context)
    throws IOException, InterruptedException
{
  int size = row.getBytes().getSize();
  SizeRows.outputRowSize(context, "Row size", size);

}


}
}
