package reconcile.hbase.query.mr;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

public class DeleteRows extends ChainableAnnotationJob {


private static final String DELETE_CFS  = "query.delete_cfs";


/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>-cf='column family name' - optional argument to specify to only delete specified column family
 *          <li>-source='source name' - optional argument to specify processing of only rows with given source name
 *          <li>-keyList='hdfs key list file' - optional argument to specify processing of only select rows by key
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new DeleteRows(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

public static final String CF_ARG = "-cf=";

@Override
public void init(JobConfig jobConfig, Job job, Scan scan)
{
	StringBuffer deleteCF = new StringBuffer("");
	for (String arg : jobConfig.getArgs())
	{
		if (arg.startsWith(CF_ARG)) {
			String value = arg.substring(CF_ARG.length());
			if (value.length() > 0 && !value.startsWith("$")) {
				deleteCF.append(value+",");
			}
		}
	}
    job.getConfiguration().set(DELETE_CFS, deleteCF.toString());
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
	return DeleteRowMapper.class;
}

public static class DeleteRowMapper extends AnnotateMapper {

private TreeSet<String> deleteCFs = new TreeSet<String>();

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
    LOG.info("Only deleting entries with source("+getSourceName()+")");

    deleteCFs = new TreeSet<String>();
    String value = context.getConfiguration().get(DELETE_CFS);
    if (value!=null && value.length() > 0) {
    	for (String cf : value.split(",")) {
    		if (cf.length() > 0) {
    			deleteCFs.add(cf);
    		}
    	}
    }
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
public void map(ImmutableBytesWritable key, Result rr, Context context)
    throws IOException, InterruptedException
{
	context.getCounter(contextHeader(), "row seen").increment(1);
	String src = DocSchema.getColumn(rr, DocSchema.srcCF, DocSchema.srcName);
	context.getCounter(contextHeader(), "row src ("+src+")").increment(1);
	if (src.equals(getSourceName()))
	{
		context.getCounter(contextHeader(), "row deleted").increment(1);
		Delete delete = new Delete(rr.getRow());
		for (String cf : deleteCFs) {
			delete.deleteFamily(cf.getBytes());
			context.getCounter(contextHeader(), "delete column family("+cf+")").increment(1);
		}
    docTable.delete(delete);
	}
}

}

}
