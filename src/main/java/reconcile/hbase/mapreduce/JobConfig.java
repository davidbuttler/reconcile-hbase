package reconcile.hbase.mapreduce;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import reconcile.hbase.table.DocSchema;

public class JobConfig {

/**
 * Command-line option to specify source name to limit rows which are processed to those with given source name
 */
static public final String SOURCE_ARG = "-source=";

public static final String SOURCE_NAME = "name.of.source";

static public final String TABLE_ARG = "-table=";

public static final String TABLE_NAME = "name.of.doc.table";

private String table = null;

private String source = null;

/**
 * Command-line option to specify HDFS file containing list of keys of rows to process [1 key per line]
 */
static public final String KEY_LIST_ARG = "-keyList=";

private String keyListFile = null;

private StringBuffer argString = new StringBuffer();

public static String usage()
{
  return "[ " + SOURCE_ARG + "<source name> | " + KEY_LIST_ARG + "<HDFS key list file> | " + TABLE_ARG
      + "<table name> ]";
}

/**
 * Mapper which operates on a Result row in HBase and does what it likes to that row [add more data to HBase, publish
 * data to Solr, etc]
 *
 * @author cottom1
 *
 */
public static abstract class Mapper<ReducerType>
    extends TableMapper<ImmutableBytesWritable, ReducerType> {

private static final String TABLE_CONF = "trinidad.hbase.mapreduce.JobConfig.Mapper.table";

private static final String SOURCE_CONF = "trinidad.hbase.mapreduce.JobConfig.Mapper.source";

private static final String KEY_LIST_CONF = "trinidad.hbase.mapreduce.JobConfig.Mapper.keyList";

private String source;

private String keyList;

private String table;

protected DocSchema docTable;

@Override
public void setup(Context context)
    throws IOException, InterruptedException
{
  super.setup(context);
  source = context.getConfiguration().get(SOURCE_CONF);

  table = context.getConfiguration().get(TABLE_CONF);
  if (table == null || table.trim().length() == 0) {
    table = source;
  }

  docTable = new DocSchema(table);

  keyList = context.getConfiguration().get(KEY_LIST_CONF);
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
}

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>-table='schema table name' - optional argument to specify name of table [defaults to source]
 *          <li>-source='source name' - optional argument to specify processing of only rows with given source name
 *          <li>-keyList='hdfs key list file' - optional argument to specify processing of only select rows by key
 *          </ol>
 */
public JobConfig(String[] args) {
  for (String arg : args) {
    if (arg.startsWith(SOURCE_ARG)) {
      String value = arg.substring(SOURCE_ARG.length());
      if (!value.startsWith("$")) {
        source = value;
      }
    }
    else if (arg.startsWith(TABLE_ARG)) {
      String value = arg.substring(TABLE_ARG.length());
      if (!value.startsWith("$")) {
        table = value;
      }
    }
    else if (arg.startsWith(KEY_LIST_ARG)) {
      String value = arg.substring(KEY_LIST_ARG.length());
      if (!value.startsWith("$")) {
        keyListFile = value;
      }
    }
  }
  if (table == null) {
    table = source;
  }
  for (String arg : args) {
    argString.append(arg + ",");
  }
}

/**
 * Method to properly initialize the Mapper/Reducer jobs based on options
 *
 * @param LOG
 * @param job
 * @param scan
 * @param mapClass
 * @throws IOException
 */
public void initTableMapperNoReducer(Log LOG, Job job, Scan scan, Class<? extends Mapper<?>> mapClass)
    throws IOException
{
  job.setJobName(job.getJobName() + "(" + argString + ")");
  if (source != null) {
    job.getConfiguration().set(Mapper.SOURCE_CONF, source);
  }
  if (table != null) {
    job.getConfiguration().set(Mapper.TABLE_CONF, table);
  }
  if (keyListFile != null) {
    job.getConfiguration().set(Mapper.KEY_LIST_CONF, keyListFile);
  }

  if (keyListFile == null) {
    LOG.info("Setting scan for source name(" + source + ") table(" + table + ")");
    initTableMapperForSource(job, scan, mapClass);
  }
  else {
    LOG.info("Operating solely on keys in HDFS file (" + keyListFile + ") source(" + source + ") table(" + table + ")");
    initTableMapperForKeyList(job, mapClass);
  }

  StringBuffer families = new StringBuffer();
  if (scan.getFamilies() != null) {
    for (byte[] family : scan.getFamilies()) {
      families.append(Bytes.toString(family) + " ");
    }
  }
  StringBuffer other = new StringBuffer();
  if (scan.getFamilyMap() != null) {
    Map<byte[], NavigableSet<byte[]>> map = scan.getFamilyMap();
    if (map != null) {
      for (byte[] family : map.keySet()) {
        if (family == null) {
          continue;
        }
        other.append(Bytes.toString(family) + "[");
        if (map.get(family) != null) {
          for (byte[] qual : map.get(family)) {
            other.append(Bytes.toString(qual) + ",");
          }
        }
        other.append("]; ");
      }
    }
  }

  String familiesValue = families.toString();
  LOG.info("Setting scan retrieve families to (" + familiesValue + ")");
  LOG.info("Setting scan retrieve families/quals to (" + other.toString() + ")");
  job.getConfiguration().set(KeyListInputFormat.SCAN_FAMILIES, familiesValue);

  TableMapReduceUtil.initTableReducerJob(getTableName(), IdentityTableReducer.class, job);
  job.setNumReduceTasks(0);
}

/**
 * Method to initialize a mapper job which will operate on rows in 'doc' table for those matching the given source name,
 * or if source is null, all rows.
 *
 * @param job
 * @param scan
 * @param mapClass
 * @throws IOException
 */
private void initTableMapperForSource(Job job, Scan scan, Class<? extends Mapper<?>> mapClass)
    throws IOException
{
  if (source != null) {
    scan.addColumn(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes());
    scan.setFilter(new SingleColumnValueFilter(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes(),
        CompareOp.EQUAL, source.getBytes()));
  }

  // set up mapper jobs
  TableMapReduceUtil.initTableMapperJob(getTableName(), scan, mapClass, ImmutableBytesWritable.class, Put.class, job);
}

/**
 * Method to initialize a mapper job which will operate only on rows in 'doc' table for keys listed in an HDFS file
 *
 * @param job
 * @param mapClass
 * @throws IOException
 */
private void initTableMapperForKeyList(Job job, Class<? extends Mapper<?>> mapClass)
    throws IOException
{
  Path inputPath = new Path(keyListFile);
  job.setMapperClass(mapClass);
  job.setInputFormatClass(KeyListInputFormat.class);
  job.setMapOutputKeyClass(ImmutableBytesWritable.class);
  job.setMapOutputValueClass(Put.class);
  FileInputFormat.addInputPath(job, inputPath);
}

/**
 * Return the name of the schema table as set by command line arguments [defaulted to source name]
 */
public String getTableName()
{
  return table;
}

/**
 * Return the source name as set by command line arguments
 *
 * @return
 */
public String getSource()
{
  return source;
}

/**
 * Return the key list file name as set by the command line arguments
 *
 * @return
 */
public String getKeyListFile()
{
  return keyListFile;
}
}
