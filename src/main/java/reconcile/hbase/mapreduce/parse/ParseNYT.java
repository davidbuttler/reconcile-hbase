package reconcile.hbase.mapreduce.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gov.llnl.text.util.MapUtil;

import reconcile.hbase.mapreduce.annotation.ReportProgressThread;
import reconcile.hbase.mapreduce.parse.nyt.NYTCorpusDocument;
import reconcile.hbase.mapreduce.parse.nyt.NYTCorpusDocumentParser;
import reconcile.hbase.table.DocSchema;

public class ParseNYT
    extends Configured
    implements Tool {

static final Log LOG = LogFactory.getLog(ParseNYT.class);

public static final String NYT_PARSER_SOURCE = "NYT_Parser_Source";

// private static final DateFormat formatId = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss.SSS");

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
    ToolRunner.run(new Configuration(), new ParseNYT(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

private static String tableName = "NYT";

private Configuration conf;

public ParseNYT() {

}

public int run(String[] args)
{
  conf = HBaseConfiguration.create();
  // important to switch spec exec off.
  // We don't want to have something duplicated.
  conf.set("mapred.map.tasks.speculative.execution", "false");

  try {

    Job job = null;
    LOG.info("Before map/reduce startup");
    job = new Job(conf, "Parse NYT");
    job.setJarByClass(ParseNYT.class);
    Scan scan = new Scan();
    scan.addColumn(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes());
    scan.addFamily(DocSchema.textCF.getBytes());
    scan
.setFilter(new SingleColumnValueFilter(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes(),
        CompareOp.EQUAL, "NYT"
        .getBytes()));

    TableMapReduceUtil.initTableMapperJob(tableName, scan, NYTParserMapper.class, ImmutableBytesWritable.class,
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

public static class NYTParserMapper
    extends TableMapper<ImmutableBytesWritable, Put> {

NYTCorpusDocumentParser nytParser;

private DocSchema docTable;

/**
 * put the original text into the doc table
 */
public NYTParserMapper() {

}

@Override
public void setup(Context context)
{
  try {
    super.setup(context);

    docTable = new DocSchema(tableName);

    nytParser = new NYTCorpusDocumentParser();

    context.getCounter("Parse NYT", "setup").increment(1L);

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
    docTable.flushCommits();
    docTable.close();
  }
}

@Override
public void map(ImmutableBytesWritable mapKey, Result row, Context context)
    throws IOException, InterruptedException
{

  String src = DocSchema.getColumn(row, DocSchema.srcCF, DocSchema.srcName);
  if (src == null || !src.equals("NYT")) {
    context.getCounter("Parse NYT", "skip -- not NYT: " + src).increment(1L);
    return;
  }
  ReportProgressThread progress = null;
  try {

    // start keep alive
    progress = ReportProgressThread.start(context, 1000);
    context.getCounter("Parse NYT", "map").increment(1L);

    String orig = DocSchema.getColumn(row, DocSchema.textCF, DocSchema.textOrig);
    if (orig == null) {
      context.getCounter("Parse NYT", "skip -- null text: " + src).increment(1L);
      return;
    }

    NYTCorpusDocument doc = nytParser.parseNYTCorpusDocumentFromString(orig, false);
    Put put = new Put(row.getRow());
    boolean addColumn = addColumns(put, doc, context);

    // System.out.println("processing file: " + f.getAbsolutePath());
      if (addColumn) {
        // Load message entry
      context.getCounter("Parse NYT", "add").increment(1L);
        docTable.put(put);
        // context.write(new ImmutableBytesWritable(putKey), put);
      }
      else {
      context.getCounter("Parse NYT", "skip -- no column added").increment(1L);
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

@SuppressWarnings({ "rawtypes", "unchecked" })
private boolean addColumns(Put put, NYTCorpusDocument doc, Context context)
{
  boolean addedColumn = false;
  int guid = doc.getGuid();
  if (guid > 0) {
    DocSchema.add(put, DocSchema.metaCF, "guid", String.valueOf(guid));
    context.getCounter("add", "guid").increment(1L);
    addedColumn = true;
  }

  String body = doc.getBody();
  if (body != null && body.length() > 0) {
    DocSchema.add(put, DocSchema.textCF, DocSchema.textRaw, body);
    context.getCounter("add", "--Body--").increment(1L);
  }
  else {
    context.getCounter("skip (null)", "--Body--").increment(1L);
  }

  for (String key : functionMap.keySet()) {
    Set<String> vals = functionMap.get(key);
    Set<String> fullList = Sets.newHashSet();
    for (String val : vals) {
      try {
        Method m = doc.getClass().getMethod(val, (Class[]) null);
        Object listObj = m.invoke(doc, (Object[]) null);
        if (listObj != null) {
          if (listObj instanceof Collection) {
            context.getCounter("add collection", val).increment(1L);
            fullList.addAll((Collection)listObj);
          }
          else {
            context.getCounter("not a collection", val).increment(1L);
          }
        }
        else {
          context.getCounter("null list obj", val).increment(1L);
        }
      }
      catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
    if (fullList.size() > 0) {
      DocSchema.add(put, DocSchema.metaCF, key, toString(fullList));
      context.getCounter("add collection list", key).increment(1L);
      addedColumn = true;
    }
  }

  for (String key : singleItemfunctionList) {
    String name = key.substring("get".length());
    try {
      Method m = doc.getClass().getMethod(key, (Class[]) null);
      Object val = m.invoke(doc, (Object[]) null);
      if (val != null) {
        String valStr = val.toString();
        if (!valStr.trim().equals("")) {
          DocSchema.add(put, DocSchema.metaCF, name, valStr);
          context.getCounter("add", name).increment(1L);
          addedColumn = true;
        }
        else {
          context.getCounter("empty val", name).increment(1L);
        }
      }
      else {
        context.getCounter("null val", name).increment(1L);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  return addedColumn;
}

private String toString(Set<String> fullList)
{
  StringBuilder b = new StringBuilder();
  for (String s : fullList) {
    b.append(s.replaceAll("\\s+", " "));
    b.append("\n");
  }
  return b.toString();
}

private static List<String> singleItemfunctionList = Lists.newArrayList("getAlternateURL", "getArticleAbstract",
    "getAuthorBiography", "getBanner", "getByline", "getColumnName", "getColumnNumber", "getCorrectionDate",
    "getCorrectionText", "getCredit", "getDateline", "getDayOfWeek", "getFeaturePage", "getGeneralOnlineDescriptors",
    "getHeadline", "getKicker", "getLeadParagraph", "getNewsDesk", "getNormalizedByline", "getOnlineHeadline",
    "getOnlineLeadParagraph", "getOnlineSection", "getPage", "getPublicationDate", "getPublicationDayOfMonth",
    "getPublicationMonth", "getPublicationYear", "getSection", "getSeriesName", "getSlug", "getUrl", "getWordCount");

private static Map<String, Set<String>> functionMap = Maps.newHashMap();
static {
  MapUtil.addToMapSet(functionMap, "NYT_Organizations", "getOrganizations");
  MapUtil.addToMapSet(functionMap, "NYT_Organizations", "getOnlineOrganizations");

  MapUtil.addToMapSet(functionMap, "NYT_People", "getOnlinePeople");
  MapUtil.addToMapSet(functionMap, "NYT_People", "getPeople");

  MapUtil.addToMapSet(functionMap, "NYT_Locations", "getLocations");
  MapUtil.addToMapSet(functionMap, "NYT_Locations", "getOnlineLocations");

  MapUtil.addToMapSet(functionMap, "NYT_Names", "getNames");

  MapUtil.addToMapSet(functionMap, "titles", "getTitles");
  MapUtil.addToMapSet(functionMap, "titles", "getOnlineTitles");

  MapUtil.addToMapSet(functionMap, "typesOfMaterial", "getTypesOfMaterial");

  MapUtil.addToMapSet(functionMap, "TaxonomicClassifiers", "getTaxonomicClassifiers");
  MapUtil.addToMapSet(functionMap, "Descriptors", "getDescriptors");
  MapUtil.addToMapSet(functionMap, "Descriptors", "getOnlineDescriptors");
  MapUtil.addToMapSet(functionMap, "BiographicalCategories", "getBiographicalCategories");
}




}

}
