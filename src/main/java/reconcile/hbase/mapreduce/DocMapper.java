package reconcile.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import reconcile.hbase.table.DocSchema;

/**
 * Abstract base mapper which initializes source, key list, and HTable for
 * working with the DocSchema as input and/or output.
 *
 * @author cottom1
 *
 * @param <ReducerType>
 */
public abstract class DocMapper<ReducerType> extends TableMapper<ImmutableBytesWritable, ReducerType>
{
	private String source;
	private String keyList;
	private String table;
	protected DocSchema docTable;

	@Override
	public void setup(Context context)
	    throws IOException, InterruptedException
	{
	  super.setup(context);
	  source = context.getConfiguration().get(JobConfig.SOURCE_CONF);

	  table = context.getConfiguration().get(JobConfig.TABLE_CONF);
	  if (table == null || table.trim().length() == 0) {
	    table = source;
	  }

	  docTable = new DocSchema(table);

	  keyList = context.getConfiguration().get(JobConfig.KEY_LIST_CONF);
	}

@Override
public void cleanup(Context context)
    throws IOException, InterruptedException
{
  try {
    docTable.flushCommits();
    docTable.close();
  }
  catch (IOException e) {
    context.getCounter(contextHeader(), "cleanup error: " + e.getMessage()).increment(1);
    e.printStackTrace();
  }

}
	public String getSourceName()
	{
	  return source;
	}

	public String getKeyList()
	{
	  return keyList;
	}

	public String getTableName()
	{
	  return table;
	}

	public String contextHeader()
	{
	  return getClass().getSimpleName();
	}

protected boolean isRowSizeTooLarge(Result row, Context context, int maxRowLength)
  {
    int size = 0;
    KeyValue[] kv = row.raw();
    for (KeyValue k : kv) {
      size += k.getLength();
    }
    System.out.println("row size: " + size);
  if (size > maxRowLength) {
    context.getCounter(contextHeader(), "skip -- row too large (" + maxRowLength + ")").increment(1);
      return true;

    }
    return false;
  }

protected boolean isRawTextInvalidSize(String rawText, Context context, int maxRawTextLength)
  {
    if (rawText == null || rawText.length() == 0) {
    context.getCounter(contextHeader(), "skip -- no raw text").increment(1);
      return true;
    }
  if (rawText.length() > maxRawTextLength) {
    context.getCounter(contextHeader(), "skip -- raw text too long (" + maxRawTextLength + ")")
          .increment(1);
      return true;
    }
    return false;
  }

}
