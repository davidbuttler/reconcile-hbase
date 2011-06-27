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
 *
 * Created on Apr 20, 2009
 *
 * $Id$
 *
 * $Log$
 */
package reconcile.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;

import reconcile.hbase.mapreduce.ChainableAnnotationJob.AnnotateMapper;


/**
 * Annotation M/R Job which is given a list of AnnotateMappers to perform the mapping operation.
 * The mapping jobs will be performed on each row in the order specified.
 *
 * @author cottom1
 *
 */
public class ChainAnnotation extends Configured implements Tool {

private static final Log LOG = LogFactory.getLog(ChainAnnotation.class);

public static final String JOB_ARG_SEPARATOR=";";
public static final String JOB_ARG="-job=";



/**
 *
 * @param args
 *          :
 *          <ol>
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new ChainAnnotation(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

private Configuration conf;

public void init(JobConfig jobConfig, Job job, Scan scan)
{
	StringBuffer mapperArg = new StringBuffer();

	for (String arg : jobConfig.getArgs())
	{
		if (arg.startsWith(JOB_ARG))
		{
			String value = arg.substring(JOB_ARG.length());
			if (!value.startsWith("$")) {
				String[] vals = value.split(JOB_ARG_SEPARATOR);
				try {
					Class<?> jobClass = Class.forName(vals[0]);
					ChainableAnnotationJob chainJob = (ChainableAnnotationJob) jobClass.newInstance();
					String[] jobArgs = new String[vals.length-1];
					for (int i=1; i<vals.length; ++i) {
						jobArgs[i-1] = vals[i];
					}
					JobConfig subJob = new JobConfig(jobArgs);
					chainJob.init(subJob, job, scan);
					if (mapperArg.length() > 0) {
						mapperArg.append(",");
					}
					mapperArg.append(chainJob.getMapperClass().getName());
				}
				catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
				catch (InstantiationException e) {
					throw new RuntimeException(e);
				}
				catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	// Set the mappers arg to be retrieved in ChainMapper
	String mappers = mapperArg.toString();
	LOG.info("Setting conf "+ChainMapper.MAPPER_NAME_CONF+" ("+mappers+")");
	job.getConfiguration().set(ChainMapper.MAPPER_NAME_CONF, mappers);
}


@Override
public int run(String[] args)
    throws Exception
{

  conf = HBaseConfiguration.create();


  JobConfig jobConfig = new JobConfig(args);

  try {

    LOG.info("Before map/reduce startup");
    Job job = new Job(conf, getClass().getSimpleName());
    job.setJarByClass(ChainAnnotation.class);
    job.setNumReduceTasks(1);
    Scan scan = new Scan();

    init(jobConfig, job, scan);

    jobConfig.initTableMapperNoReducer(LOG, job, scan, ChainMapper.class);

    LOG.info("Started (" + Joiner.on(",").join(args) + ")");
    job.waitForCompletion(true);
    LOG.info("After map/reduce completion");

  }
  catch (Exception e) {
    e.printStackTrace();
    return 1;
  }

  return 0;
}


public static class ChainMapper extends AnnotateMapper
{
	private static String MAPPER_NAME_CONF = "trinidad.hbase.mapreduce.JobConfig.ChainMapper.mappers";

	ArrayList<AnnotateMapper> mappers = new ArrayList<AnnotateMapper>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		String mapperNames = context.getConfiguration().get(MAPPER_NAME_CONF);
		LOG.info("Begin configuring mappers "+MAPPER_NAME_CONF+" value("+mapperNames+")");
		for (String name : mapperNames.split(",")) {
			if (name==null || name.length()==0) {
        continue;
      }

			try {
				LOG.info("Configuring mapper ("+name+") for chaining");
				// Construct and initialize the chained mappers
				Class<?> classType = Class.forName(name);
				AnnotateMapper mapper = (AnnotateMapper) classType.newInstance();
				mapper.setup(context);
				mappers.add(mapper);
				context.getCounter(contextHeader(), "mapper initialized").increment(1);
			}
			catch (ClassCastException e) {
				e.printStackTrace();
			}
			catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			catch (InstantiationException e) {
				e.printStackTrace();
			}
			catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context)
	    throws IOException, InterruptedException
	{
		context.getCounter(contextHeader(), "row started").increment(1);
		for (AnnotateMapper mapper : mappers)
		{
			try {
				mapper.map(key, value, context);
			}
			catch (IOException e) {
				context.getCounter(contextHeader(), "row process error").increment(1);
				throw e;
			}
			catch (InterruptedException e) {
				context.getCounter(contextHeader(), "row process error").increment(1);
				throw e;
			}
		}
		context.getCounter(contextHeader(), "row completed").increment(1);
	}
}

}
