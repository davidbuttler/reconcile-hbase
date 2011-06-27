package reconcile.hbase.table;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import junit.framework.Assert;
import junit.framework.TestCase;

public class DocSchemaTest extends TestCase 
{

	public static final String tableName = "test";
	
	public static final String textOrig = "This is my original test text";
	public static final String textRaw = "This is raw text";

	public static final String key = DigestUtils.shaHex(textOrig);
	
	
	public static void setupData() 
		throws IOException
	{
		DocSchema table = new DocSchema(tableName, true); 
		
		Put put = new Put(key.getBytes());
		
		put.add(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes(), tableName.getBytes());
		put.add(DocSchema.srcCF.getBytes(), DocSchema.srcId.getBytes(), key.getBytes());
		put.add(DocSchema.textCF.getBytes(), DocSchema.textOrig.getBytes(), textOrig.getBytes());
		put.add(DocSchema.textCF.getBytes(), DocSchema.textRaw.getBytes(), textRaw.getBytes());
		
		table.put(put);
		table.close();
	}
	
	public void setUp()
		throws IOException
	{
		setupData();
	}
	
	public void test() 
		throws IOException
	{
		DocSchema table = new DocSchema(tableName);
		Assert.assertEquals(tableName, table.getTableName());
		
		Assert.assertTrue(table.exists(key.getBytes()));
		
		Get get = new Get(key.getBytes());
		get.addFamily(DocSchema.srcCF.getBytes());
		get.addFamily(DocSchema.textCF.getBytes());
		Result row = table.get(get);
		
		// validate row data
		Assert.assertEquals(tableName, DocSchema.getColumn(row, DocSchema.srcCF, DocSchema.srcName));
		Assert.assertEquals(key, DocSchema.getColumn(row, DocSchema.srcCF, DocSchema.srcId));
		Assert.assertEquals(textOrig, DocSchema.getColumn(row, DocSchema.textCF, DocSchema.textOrig));
		Assert.assertEquals(textRaw, DocSchema.getColumn(row, DocSchema.textCF, DocSchema.textRaw));
		
		table.close();
	}
	
}
