package reconcile.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import reconcile.hbase.table.DocSchema;

public class FiltersTests extends TestCase {

	public static final String GT_DOC_KEY="e2a4b6491358332a3d8ae232683195a1a6da82da";
	public static final String PUBMED_DOC_KEY="1eecbfc97458d266dbef943ad922c6fd903358b1";

	protected boolean gtFound=false, pubmedFound=false;

protected DocSchema gtTable;

protected DocSchema pubmedTable;
	protected Scan scan;

	@Override
  protected void setUp() throws Exception {
		super.setUp();
  gtTable = new DocSchema("GT");
  pubmedTable = new DocSchema("PubMed");
	    scan = new Scan();
	}

	@Override
  protected void tearDown() throws Exception {
		super.tearDown();
  gtTable.close();
  pubmedTable.close();
	}

	protected boolean validateSrc(Result result, String expected)
	{
		String actual = DocSchema.getColumn(result, DocSchema.srcNameCol);
		Assert.assertEquals(expected, actual);

		return true;
	}

	protected void scanAndValidate() throws IOException
	{
		scan.addFamily(DocSchema.srcCF.getBytes());

  // gt scan
  ResultScanner scanner = gtTable.getScanner(scan);
  Iterator<Result> iter = scanner.iterator();
  while (iter.hasNext()) {
    Result result = iter.next();
    String id = Bytes.toString(result.getRow());
    if (id.equals(GT_DOC_KEY)) {
      gtFound = validateSrc(result, "GT2009");
    }
    else if (id.equals(PUBMED_DOC_KEY)) {
      pubmedFound = validateSrc(result, "PubMed");
    }
    else {
      Assert.fail("unexpected result returned key(" + id + ")");
    }
  }
  scanner = pubmedTable.getScanner(scan);
  iter = scanner.iterator();
  while (iter.hasNext()) {
    Result result = iter.next();
    String id = Bytes.toString(result.getRow());
    if (id.equals(GT_DOC_KEY)) {
      gtFound = validateSrc(result, "GT2009");
    }
    else if (id.equals(PUBMED_DOC_KEY)) {
      pubmedFound = validateSrc(result, "PubMed");
    }
    else {
      Assert.fail("unexpected result returned key(" + id + ")");
    }
  }

	    Assert.assertTrue(gtFound);
	    Assert.assertTrue(pubmedFound);
	}

	public void testKeysCollection() throws IOException
	{
		ArrayList<String> keys = new ArrayList<String>();
		keys.add(GT_DOC_KEY);
		keys.add(PUBMED_DOC_KEY);
		Filters.keys(scan, keys);

		scanAndValidate();
	}

	public static void createKeysFile(Path file)
		throws IOException
	{
		HBaseConfiguration conf = new HBaseConfiguration();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream os = null;
		try {
			os = fs.create(file, true);
			os.writeBytes(GT_DOC_KEY+"\n");
			os.writeBytes(PUBMED_DOC_KEY+"\n");
			os.flush();
		}
		finally {
			if (os!=null) {
        os.close();
      }
		}
	}

        public static void deleteFile(Path file)
		throws IOException
	{
		HBaseConfiguration conf = new HBaseConfiguration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(file, true);
	}

	public void testKeysFile() throws IOException
	{
		Path testFile = new Path("/test/testKeysFile.txt");
		createKeysFile(testFile);

		Filters.keys(scan, testFile.getName());

		scanAndValidate();

		deleteFile(testFile);
	}
}
