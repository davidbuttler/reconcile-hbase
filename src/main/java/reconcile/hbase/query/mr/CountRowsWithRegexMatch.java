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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CountRowsWithRegexMatch
    extends Configured
    implements Tool {

private static final Log LOG = LogFactory.getLog(CountRowsWithRegexMatch.class);

private static final String TABLE_NAME = "query.table_name";

private static final String COL_NAME = "query.col_name";

private static final String QUAL_NAME = "query.qual_name";

private static final String QUERY_PATTERN = "query.pattern";


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
    ToolRunner.run(new Configuration(), new CountRowsWithRegexMatch(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

public int run(String[] args)
    throws Exception
{
  if (args.length != 4) {
    System.out.println("usage: CountRows <table name> <column name> <qual name> <pattern>");
    return 1;
  }
  Configuration conf = HBaseConfiguration.create();

  try {
    String tableName = args[0];
    conf.set(TABLE_NAME, tableName);
    String colName = args[1];
    conf.set(COL_NAME, colName);
    String qualName = args[2];
    conf.set(QUAL_NAME, qualName);
    String pattern = args[3];
    conf.set(QUERY_PATTERN, pattern);

    Job job = null;
    LOG.info("Before map/reduce startup");
    job = new Job(conf, "query: count rows");
    job.setJarByClass(CountRowsWithRegexMatch.class);
    job.setNumReduceTasks(1);
    job.getConfiguration().set(TABLE_NAME, args[0]);

    Scan scan = new Scan();
    scan.addColumn(colName.getBytes(), qualName.getBytes());
    scan.setFilter(new SingleColumnValueFilter(colName.getBytes(), qualName.getBytes(), CompareOp.EQUAL,
        new RegexStringComparator(pattern)));

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

  context.getCounter("CountRows", "row count").increment(1);
}


}
}
