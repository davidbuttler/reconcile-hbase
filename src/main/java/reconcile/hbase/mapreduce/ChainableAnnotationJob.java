package reconcile.hbase.mapreduce;

import static reconcile.hbase.mapreduce.JobConfig.SOURCE_ARG;
import static reconcile.hbase.mapreduce.JobConfig.SOURCE_NAME;
import static reconcile.hbase.mapreduce.annotation.AnnotationUtils.getAnnotationStr;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import reconcile.hbase.mapreduce.JobConfig.Mapper;
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
	public static abstract class AnnotateMapper extends Mapper<Put>
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
	 * Initialize the M/R job and HBase scan based on command-line arguments
	 *
	 * @param args
	 * @param job
	 * @param scan
	 */
	public abstract void init(String[] args, Job job, Scan scan);

	/**
	 * Get the class to run as the Mapper
	 * @return
	 */
	public abstract Class<? extends AnnotateMapper> getMapperClass();

	public static final Log LOG = LogFactory.getLog(ChainableAnnotationJob.class);

	private HBaseConfiguration conf;

protected String source;

	@Override
	public int run(String[] args)
	    throws Exception
	{
	  conf = new HBaseConfiguration();
	  // important to switch spec exec off.
	  // We don't want to have something duplicated for perfomance reasons.
	  conf.set("mapred.map.tasks.speculative.execution", "false");

	  // since our parse takes so long, we don't want to cache rows -- the scanner might time out
	  conf.set("hbase.client.scanner.caching", "1");

	  JobConfig jobConfig = new JobConfig(args);

	  try {

	    LOG.info("Before map/reduce startup");

	    Job job = new Job(conf, getClass().getSimpleName());
	    job.setJarByClass(this.getClass());

	    Scan scan = new Scan();

	    init(args, job, scan);

	    jobConfig.initTableMapperNoReducer(LOG, job, scan, getMapperClass());

    LOG.info("Started " + source);
	    job.waitForCompletion(true);
	    LOG.info("After map/reduce completion");
	  }
	  catch (Exception e) {
	    e.printStackTrace();
	    return 1;
	  }

	  return 0;
	}

public void setSource(String[] args, Job job)
{
  boolean set = false;
  for (String arg : args) {
    if (arg.startsWith(SOURCE_ARG)) {
      source = arg.substring(SOURCE_ARG.length());
      job.getConfiguration().set(SOURCE_NAME, source);
      set = true;
    }
  }
  if (!set) throw new RuntimeException("source must be set in args by: " + SOURCE_ARG + "<src name>");

}

}
