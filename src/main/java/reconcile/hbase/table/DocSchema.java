package reconcile.hbase.table;

import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_BLOCKCACHE;
import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_BLOOMFILTER;
import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_IN_MEMORY;
import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_TTL;
import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_VERSIONS;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.solr.schema.DateField;
import org.apache.tools.ant.filters.StringInputStream;

import com.google.common.base.Charsets;

import reconcile.data.AnnotationReaderBytespan;
import reconcile.data.AnnotationSet;
import reconcile.data.AnnotationWriterBytespan;
import reconcile.data.Document;
import reconcile.general.Constants;

/**
 * This class documents the schema of the doc table: <br/>
 * Table name:
 * <ul>
 * <li>doc</li>
 * </ul>
 * Column Families:
 * <ul>
 * <li>src</li>
 * <li>meta</li>
 * <li>text</li>
 * <li>annotations</li>
 * </ul>
 *
 * Known column identifiers: (in the format CF:id)
 * <ul>
 * <li>src:name</li>
 * <li>src:id</li>
 * <li>text:mimeType</li>
 * <li>text:orig</li>
 * <li>text:raw</li>
 * <li>text:title</li>
 * <li>text:fileName</li>
 * <li>text:lineNumber</li>
 * <li>text:reference</li>
 * <li>annotations:paragraph</li>
 * <li>annotations:sentence</li>
 * <li>annotations:pos</li>
 * <li>annotations:token</li>
 * <li>annotations:parse</li>
 * <li>annotations:dep</li>
 * <li>annotations:ne</li>
 *
 *
 * @author buttler1
 *
 */
public class DocSchema {


/**
 * source column Family <br>
 * Note: the row key should be src:name ":" src:id
 */
public static final String srcCF = "src";

public static final String srcName = "name";

public static final String srcNameCol = "src:name";

public static final String srcId = "id";

public static final String srcIdCol = "src:id";

/**
 * meta column Family
 */
public static final String metaCF = "meta";

public static final String ingestDate = "ingestDate";

public static final String ingestDateCol = metaCF+":"+ingestDate;

/**
 * text column Family
 */
public static final String textCF = "text";

public static final String textOrig = "orig";

public static final String textType = "mimeType";

public static final String textRaw = "raw";

public static final String textRawCol = "text:raw";

public static final String textTitle = "title";

public static final String textTitleCol = textCF+":"+textTitle;

public static final String textFileName = "fileName";

public static final String textFileNameCol = textCF+":"+textFileName;

public static final String textByteOffset = "byteOffset";

public static final String textByteOffsetCol = textCF+":"+textByteOffset;

public static final String textReference = "reference";

public static final String textReferenceCol = textCF+":"+textReference;

/**
 * annotation column Family
 */
public static final String annotationsCF = "annotations";

public static final String annotationsParagraph = Constants.PAR; // "paragraph";

public static final String annotationsSentence = Constants.SENT; // "sentence";

public static final String annotationsToken = Constants.TOKEN; // "token";

public static final String annotationsPOS = Constants.POS; // "pos";

public static final String annotationsParse = Constants.PARSE; // "parse";

public static final String annotationsDep = Constants.DEP; // "dep";

public static final String annotationsNE = Constants.NE; // "ne";

public static final AnnotationReaderBytespan reader = new AnnotationReaderBytespan();


public static void addDocumet(Put mut, Document doc)
{
  mut.add(textCF.getBytes(), textRaw.getBytes(), doc.getText().getBytes());

  for (String key : doc.getMetaDataKeys()) {
    String val = doc.getMetaData(key);
    if (val != null) {
      mut.add(metaCF.getBytes(), key.getBytes(), val.getBytes());
    }
  }
  AnnotationWriterBytespan writer = new AnnotationWriterBytespan();
  for (String key : doc.getAnnotationSetNames()) {
    AnnotationSet set = doc.getAnnotationSet(key);
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw);
    writer.write(set, out);
    out.flush();
    mut.add(annotationsCF.getBytes(), key.getBytes(), sw.toString().getBytes());
  }

}

protected String myTableName;

public String getTableName()
{
  return myTableName;
}

/**
 * Create the table in hbase
 *
 * @return
 * @throws IOException
 */
protected final void createTable()
    throws IOException
{
  Configuration conf = HBaseConfiguration.create();
  HConnection connector = HConnectionManager.getConnection(conf);
  createTable(connector);

}

/**
 * Create the table in hbase, given a connection
 *
 * @return
 * @throws IOException
 */
protected void createTable(HConnection connector)
    throws IOException
{

  Configuration conf = HBaseConfiguration.create();
  HBaseAdmin admin = new HBaseAdmin(conf);
  if (admin.tableExists(myTableName.getBytes())) return;

  HTableDescriptor docDesc = new HTableDescriptor(myTableName.getBytes());
  docDesc.addFamily(new HColumnDescriptor(srcCF.getBytes(), DEFAULT_VERSIONS, "GZ", DEFAULT_IN_MEMORY,
      DEFAULT_BLOCKCACHE, DEFAULT_TTL, DEFAULT_BLOOMFILTER));
  docDesc.addFamily(new HColumnDescriptor(metaCF.getBytes(), DEFAULT_VERSIONS, "GZ", DEFAULT_IN_MEMORY,
      DEFAULT_BLOCKCACHE, DEFAULT_TTL, DEFAULT_BLOOMFILTER));
  docDesc.addFamily(new HColumnDescriptor(textCF.getBytes(), DEFAULT_VERSIONS, "GZ", DEFAULT_IN_MEMORY,
      DEFAULT_BLOCKCACHE, DEFAULT_TTL, DEFAULT_BLOOMFILTER));
  docDesc.addFamily(new HColumnDescriptor(annotationsCF.getBytes(), DEFAULT_VERSIONS, "GZ", DEFAULT_IN_MEMORY,
      DEFAULT_BLOCKCACHE, DEFAULT_TTL, DEFAULT_BLOOMFILTER));

  admin.createTable(docDesc);


}


public static final String COUNT_REGIONS_ARG="-getRegionCount";

public static void main(String[] args)
{
	String tableName = args[0];

	try {
	  	for (String arg : args) {
	  		if (arg.equals(COUNT_REGIONS_ARG)) {
	  			DocSchema doc = new DocSchema(tableName);
	  			int numRegions = doc.getNumberOfRegions();
	  			System.out.println("There are ("+numRegions+") regions in table("+tableName+")");
	  			return;
	  		}
	  	}

	  	System.out.println("Create table named: " + tableName);
	  	new DocSchema(tableName, true);
  }
  catch (IOException e) {
    e.printStackTrace();
  }
}

/**
 * get the raw text from a row as a string
 *
 * @param row
 * @return string
 */
public static String getRawText(Result row)
{
  try {
    // Get the raw text
    byte[] rawBytes = row.getValue(textCF.getBytes(), textRaw.getBytes());
    if (rawBytes == null) return null;
    String rawText = new String(rawBytes, HConstants.UTF8_ENCODING);
    return rawText;
  }
  catch (UnsupportedEncodingException e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }

}

public static String getMetaData(Result row, String metaDataField)
{
  try {
    // Get the raw text
    byte[] bytes = row.getValue(metaCF.getBytes(), metaDataField.getBytes());
    if (bytes == null) return null;
    String text = new String(bytes, HConstants.UTF8_ENCODING);
    return text;
  }
  catch (UnsupportedEncodingException e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }
}

public static String getColumn(Result row, String fieldName)
{
  String family = null;
  String qual = null;
  int index = fieldName.indexOf(":");
  if (index < 0) {
    family = metaCF;
    qual = fieldName;
  }
  else {
    family = fieldName.substring(0, index);
    qual = fieldName.substring(index + 1);
  }
  try {
    // Get the raw text
    byte[] bytes = row.getValue(family.getBytes(), qual.getBytes());
    if (bytes == null) return null;
    String text = new String(bytes, HConstants.UTF8_ENCODING);
    return text;
  }
  catch (UnsupportedEncodingException e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }
}

public static String getColumn(Result row, String family, String qualifier)
{
  try {
    // Get the raw text
    byte[] bytes = row.getValue(family.getBytes(), qualifier.getBytes());
    if (bytes == null) return null;
    String text = new String(bytes, HConstants.UTF8_ENCODING);
    return text;
  }
  catch (UnsupportedEncodingException e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }
}

public static AnnotationSet getAnnotationSet(Result row, String annotationSetName)
{
  try {
    AnnotationSet annotationSet = null;
    byte[] bytes = row.getValue(annotationsCF.getBytes(), annotationSetName.getBytes());
    if (bytes == null || bytes.length == 0) return null;
    String parseStr = new String(bytes, HConstants.UTF8_ENCODING);
    annotationSet = reader.read(new StringInputStream(parseStr), annotationSetName);
    return annotationSet;
  }
  catch (UnsupportedEncodingException e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }
}

public static AnnotationSet getAnnotationSet(KeyValue kv)
{
  try {
    AnnotationSet annotationSet = null;
    byte[] bytes = kv.getValue();
    if (bytes == null || bytes.length == 0) return null;
    String parseStr = new String(bytes, HConstants.UTF8_ENCODING);
    annotationSet = reader.read(new StringInputStream(parseStr), new String(kv.getQualifier(), Charsets.UTF_8));
    return annotationSet;
  }
  catch (UnsupportedEncodingException e) {
    e.printStackTrace();
    throw new RuntimeException(e);
  }
}

private static final DateField dateField = new DateField();
public static String formatSolrDate(Date date)
{
	if (date==null) return null;
	return dateField.toInternal(date)+"Z";
}

public static void addIngestDate(Put put)
{
	Date time = Calendar.getInstance().getTime();
    String ingestDateValue = formatSolrDate(time);
    put.add(metaCF.getBytes(), ingestDate.getBytes(), ingestDateValue.getBytes());
}

public static void add(Put put, String col, String qual, String data)
{
  if (data == null || data.trim().equals("")) return;
  put.add(col.getBytes(), qual.getBytes(), data.getBytes());
}

public static void add(Put put, String col, String qual, byte[] data)
{
  if (data == null || data.length == 0) return;
  put.add(col.getBytes(), qual.getBytes(), data);
}

public static boolean add(Put put, String col, String qual, String data, Counter counter)
{
  if (data == null || data.trim().equals("")) return false;
  put.add(col.getBytes(), qual.getBytes(), data.getBytes());
  counter.increment(1);
  return true;
}


public static void add(Put put, String col, String qual, Double value)
{
  if (value == null) {
    value = Double.valueOf(0);
  }
  put.add(col.getBytes(), qual.getBytes(), Bytes.toBytes(value));
}

protected HTable mTable;

public DocSchema(String tableName)
	throws IOException
{
	init(tableName, false);
}

public DocSchema(String tableName, boolean attemptCreateTable)
	throws IOException
{
	init(tableName, attemptCreateTable);
}

private void init(String tableName, boolean attemptCreateTable)
	throws IOException
{
	if (tableName == null) throw new IOException("table name must not be null");
	tableName = tableName.trim();
	if (tableName.length()==0) throw new IOException("table name must not be empty");
	myTableName = tableName;

	if (attemptCreateTable) {
    createTable();
  }

	Configuration config = HBaseConfiguration.create();
	mTable = new HTable(config, myTableName.getBytes());

	if (mTable == null) throw new NullPointerException("HTable is null for table name("+myTableName+")");
}

public HTable getTable()
    throws IOException
{
  return mTable;
}

public void flushCommits()
    throws IOException
{
  if (mTable != null) {
    mTable.flushCommits();
  }
}
public void close()
    throws IOException
{
  if (mTable != null) {
    mTable.flushCommits();
    mTable.close();
  }
}

public void put(Put p)
    throws IOException
{
  mTable.put(p);
}

public void put(List<Put> putList)
    throws IOException
{
  mTable.put(putList);
}

public Result get(Get get)
    throws IOException
{
  return mTable.get(get);
}

public void delete(Delete delete)
    throws IOException
{
  mTable.delete(delete);
}

public ResultScanner getScanner(Scan scan)
    throws IOException
{
  return mTable.getScanner(scan);
}

/**
 * Return whether or not the given row exists in table
 * @param row
 * @return
 */
public boolean exists(byte[] row)
{
	Get get = new Get(row);
	try {
		return mTable.exists(get);
	}
	catch (IOException e) {
		e.printStackTrace();
	}
	return false;
}

/**
 * Add the given row id as another reference to the given PUT record.  Return true if the PUT will
 * append previous data [PUT row existed], otherwise return false
 *
 * @param put
 * @param referenceRowId
 * @return
 */
public boolean addReference(Put put, String referenceRowId)
{
	boolean alreadyExists = false;
	boolean alreadyContains = false;

	StringBuffer references = new StringBuffer();
	try {
		Get get = new Get(put.getRow());

		// Check if this row exists already in HBase table
		if (mTable.exists(get))
		{
			get.addColumn(textCF.getBytes(), textReference.getBytes());
			Result value = mTable.get(get);
			String previousReference = Bytes.toString(value.getValue(textCF.getBytes(), textReference.getBytes()));
			if (previousReference!=null) {
				references.append(previousReference+";");
			}
			alreadyExists = true;

			// Track if this particular reference has been 'seen' before
      if (previousReference != null && previousReference.contains(referenceRowId)) {
				alreadyContains = true;
			}
		}
	}
	catch (IOException e) {
		e.printStackTrace();
	}

	// Only add this reference to list if it has not been 'seen' before
	if (!alreadyContains)
	{
		references.append(referenceRowId);

		// each reference is its own new field, so that we always add never overwrite
		put.add(textCF.getBytes(), textReference.getBytes(), references.toString().getBytes());
	}

    return alreadyExists;
}

/**
 * Computes the number of regions per table.
 *
 * @return The total number of regions per table.
 * @throws IOException When the table is not created.
 */
public int getNumberOfRegions()
	throws IOException
{
	return mTable.getStartKeys().length;
}


}
