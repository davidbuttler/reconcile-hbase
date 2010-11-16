package reconcile.hbase.mapreduce.ingest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import gov.llnl.text.util.FileUtils;

import reconcile.hbase.mapreduce.annotation.ReportProgressThread;
import reconcile.hbase.table.DocSchema;

public class ImportNYT
    extends Configured
    implements Tool {

static final Log LOG = LogFactory.getLog(ImportNYT.class);

public static final String PARSER_SOURCE = "NYT_Parser_Source";

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>HDFS path for list of input files (also in HDFS)
 *          <li>source name
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new ImportNYT(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

private HBaseConfiguration conf;

private static String tableName = "NYT";

public ImportNYT() {

}

public int run(String[] args)
{
  String inputPath = args[0];
  conf = new HBaseConfiguration();
  conf.set(PARSER_SOURCE, args[1]);
  // important to switch spec exec off.
  // We don't want to have something duplicated.
  conf.set("mapred.map.tasks.speculative.execution", "false");

  try {

    Job job = null;
    LOG.info("Before map/reduce startup");
    job = new Job(conf, "Import NYT");
    job.setJarByClass(ImportNYT.class);
    job.setMapperClass(NYTMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(TextInputFormat.class);

    // set the path for the input file which contains a list of files to operate over
    FileInputFormat.addInputPath(job, new Path(inputPath));
    // we want to split the input file into individual lines (paths to input files) for maps to operate on
    // so that we can have one input file per map
    // each line is 57 characters long, so if we extend a single character past we can get one line per map mostly?
    FileInputFormat.setMaxInputSplitSize(job, 38L);

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

public static class NYTMapper
    extends Mapper<LongWritable, Text, Text, Text> {

private static final String XML_MIME_TYPE = "text/xml";

private DocSchema docTable;

private String source;

// private JobConf mJobConf;

// private static final Pattern pSpace = Pattern.compile("\\s");
/**
*
*/
public NYTMapper() {

  try {
    docTable = new DocSchema(tableName);

  }
  catch (Exception e) {
    e.printStackTrace();
    ImportNYT.LOG.info(e.getMessage());
    throw new RuntimeException(e);
  }

}

@Override
public void setup(Context context)
{
  try {
    super.setup(context);

    source = context.getConfiguration().get(PARSER_SOURCE, "NYT");

    context.getCounter("ImportNYT", "setup").increment(1L);

  }
  catch (FileNotFoundException e) {
    e.printStackTrace();
  }
  catch (IOException e) {
    e.printStackTrace();
  }
  catch (InterruptedException e) {
    e.printStackTrace();
  }

}

@Override
protected void cleanup(Context context1)
    throws IOException, InterruptedException
{
  if (docTable != null) {
    docTable.close();
  }
}
@Override
public void map(LongWritable mapKey, Text path, Context context)
    throws IOException, InterruptedException
{
  context.getCounter("ImportNYT", "map").increment(1L);
  ReportProgressThread progress = null;
  try {
    // start keep alive
    progress = ReportProgressThread.start(context, 1000);

    System.out.println("processing: '" + path.toString() + "'");
    String p = path.toString();
    // File inputFile = new File(p);
    // FileInputStream is = new FileInputStream(inputFile);
    Path pPath = new Path(p);
    Path parent = pPath.getParent();
    Configuration config = context.getConfiguration();
    FileSystem fs = FileSystem.get(config);
    InputStream is = fs.open(pPath);
    GZIPInputStream gis = new GZIPInputStream(is);
    TarInputStream tis = new TarInputStream(gis);
    TarEntry tarEntry = tis.getNextEntry();
    while (tarEntry != null) {
      if (tarEntry.isDirectory()) {
        context.getCounter("ImportNYT", "skip dir").increment(1L);
        tarEntry = tis.getNextEntry();
        continue;
      }
      context.getCounter("ImportNYT", "import file").increment(1L);
      String file = FileUtils.readFile(new BufferedReader(new InputStreamReader(tis)));
      String key = parent.getName() + tarEntry.getName();
      byte[] putKey = DigestUtils.shaHex(key).getBytes();
      Put put = new Put(putKey);
      DocSchema.add(put, DocSchema.srcCF, DocSchema.srcName, source);
      DocSchema.add(put, DocSchema.srcCF, DocSchema.srcId, key);
      DocSchema.add(put, DocSchema.srcCF, "parent", parent.getName());
      DocSchema.add(put, DocSchema.srcCF, "file", tarEntry.getName());
      DocSchema.add(put, DocSchema.textCF, DocSchema.textType, XML_MIME_TYPE);
      DocSchema.add(put, DocSchema.textCF, DocSchema.textOrig, file);
      DocSchema.addIngestDate(put);

      // Load message entry
      context.getCounter("ImportNYT", "add").increment(1L);
      docTable.put(put);

      tarEntry = tis.getNextEntry();
    }

  }
  catch (IOException e) {
    e.printStackTrace();
  }
  finally {
    if (progress != null) {
      progress.interrupt();
    }
  }

}



}
}
