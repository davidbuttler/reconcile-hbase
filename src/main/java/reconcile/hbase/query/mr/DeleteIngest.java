package reconcile.hbase.query.mr;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;

import reconcile.hbase.mapreduce.ChainableAnnotationJob;
import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

public class DeleteIngest extends ChainableAnnotationJob {




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
    ToolRunner.run(new Configuration(), new DeleteIngest(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}


@Override
public void init(JobConfig jobConfig, Job job, Scan scan)
{

}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
  return DeleteIngestMapper.class;
}

public static class DeleteIngestMapper
    extends AnnotateMapper {

private Pattern ingestPattern;

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
    LOG.info("Only deleting entries with source("+getSourceName()+")");
    ingestPattern = Pattern.compile("2010.*");
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
    boolean deleted = false;
    Map<byte[], byte[]> map = rr.getFamilyMap("meta".getBytes());
    for (byte[] k : map.keySet()) {
      String kStr = new String(k, Charsets.UTF_8);
      if (ingestPattern.matcher(kStr).matches()) {
        delete.deleteColumn("meta".getBytes(), k);
        context.getCounter(contextHeader(), "delete meta ingest").increment(1);
        deleted = true;
      }
		}
    if (deleted) {
      docTable.delete(delete);
    }
	}
}

}

}
