/*
 * Copyright (c) 2010, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
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

import static reconcile.hbase.mapreduce.annotation.AnnotationUtils.getAnnotationStr;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import reconcile.data.AnnotationSet;
import reconcile.hbase.table.DocSchema;

public abstract class ChainableAnnotationJob extends Configured implements Tool
{


	/**
	 * Mapper which operates on a Result row in HBase and adds annotations to HBase, and Result
	 * for further downstream processing.
	 *
	 * @author cottom1
	 *
	 */
	public static abstract class AnnotateMapper extends DocMapper<Put>
	{
		/*
		 * Simply overridden to make map public
		 */
		@Override
		public void map(ImmutableBytesWritable key, Result value, Context context)
		    throws IOException, InterruptedException
		{
			super.map(key, value, context);
		}

		protected void addToResult(Result value, String colFamily, String colQual, byte[] data)
		{
			// Add entry to result for downstream processing
			NavigableMap<byte[], NavigableMap<byte[],byte[]>> values = value.getNoVersionMap();
			NavigableMap<byte[], byte[]> familyMap = values.get(colFamily.getBytes());
			if (familyMap == null) {
				familyMap = new TreeMap<byte[],byte[]>();
				values.put(colFamily.getBytes(), familyMap);
			}
			familyMap.put(colQual.getBytes(), data);
		}

		protected boolean addField(Result value, Put put, String col, String qual, String data, Counter counter)
		{
			if (DocSchema.add(put, col, qual, data, counter)) {
				addToResult(value, col, qual, data.getBytes());
				return true;
			}
			return false;
		}

		protected void addField(Result value, Put put, String colFamily, String colQual, byte[] data)
		{
			// Add entry to HBase
			DocSchema.add(put, colFamily, colQual, data);
			addToResult(value, colFamily, colQual, data);
		}

		protected void addAnnotation(Result value, Put put, AnnotationSet set, String name)
		{
			String data = getAnnotationStr(set);
			addField(value, put, DocSchema.annotationsCF, name, data.getBytes());
		}
	}

/**
 * Initialize the M/R job and HBase scan based on command-line arguments. Common items to set in this method:
 * <ul>
 * <li>scan (start, stop, filter)
 * <li>parameters that the child map jobs might need
 * <li>scanner caching (default is pushed to 1 to compute-bound tasks; I/O bound tasks could go to 10000)
 * </ul>
 * 
 * @param args
 * @param job
 * @param scan
 */
	public abstract void init(JobConfig jobConfig, Job job, Scan scan);

	/**
	 * Any post M/R job work
	 */
	public void finish()
	{
		LOG.info("not overridden. No post M/R tasks to complete.");
	}
	
	/**
	 * Get the class to run as the Mapper
	 * @return
	 */
	public abstract Class<? extends AnnotateMapper> getMapperClass();

	public static final Log LOG = LogFactory.getLog(ChainableAnnotationJob.class);

private Configuration conf;

	@Override
	public int run(String[] args)
	    throws Exception
	{
		conf = HBaseConfiguration.create();
	  // important to switch spec exec off.
	  // We don't want to have something duplicated for perfomance reasons.
	  conf.set("mapred.map.tasks.speculative.execution", "false");

	  // since our parse takes so long, we don't want to cache rows -- the scanner might time out
	  conf.set("hbase.client.scanner.caching", "1");

	  JobConfig jobConfig = new JobConfig(args);

	  Scan scan = new Scan();

	  int status = 0;
	  try {

	    LOG.info("Before map/reduce startup");

	    Job job = new Job(conf, getClass().getSimpleName());
	    job.setJarByClass(this.getClass());

	    init(jobConfig, job, scan);

	    jobConfig.initTableMapperNoReducer(LOG, job, scan, getMapperClass());

	    LOG.info("Started ");
	    job.waitForCompletion(true);
	    if (!job.isSuccessful())
	    	status = 1;
	    LOG.info("After map/reduce completion");
	    
	    finish();
	  }
	  catch (Exception e) {
	    e.printStackTrace();
	    status = 1;
	  }

	  LOG.info("Return run status(0=success,1=failure)("+status+")");
	  return status;
	}

}
