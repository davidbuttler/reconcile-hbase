/*
 * Copyright (c) 2008, Lawrenimport java.io.IOException;
 * 
 * import org.apache.hadoop.hbase.HBaseConfiguration; import org.apache.hadoop.hbase.KeyValue; import
 * org.apache.hadoop.hbase.MasterNotRunningException; import org.apache.hadoop.hbase.client.HBaseAdmin; import
 * org.apache.hadoop.hbase.client.HTable; import org.apache.hadoop.hbase.client.Result; import
 * org.apache.hadoop.hbase.client.ResultScanner; import org.apache.hadoop.hbase.client.Scan; import
 * org.apache.hadoop.hbase.filter.CompareFilter; import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; import
 * org.apache.hadoop.hbase.util.Bytes; A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public
 * License for more details. You should have received a copy of the GNU General Public License along with this program;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full
 * text see license.txt
 */
package reconcile.hbase.query;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class ScanColumn {

private static String table = "example";

private static String FETCH_ARG="-fetch=";
private static String FILTER_ARG="-filter=";
private static String NUM_ROWS_ARG="-numRows=";

public static class Column
{
	String value;
	String[] vals;

	public Column(String value) {
		this.value = value;
		this.vals = value.trim().replaceAll("=", "").split(":");
	}
	public String getFamily() {
		return vals[0];
	}
	public String getQualifier() {
		if (vals.length < 2) return null;
		return vals[1];
	}
	public String getValue() {
		if (vals.length < 3) return null;
		return vals[2];
	}
	@Override
  public String toString() {
		return value;
	}
}

public static String usage()
{
	return "Usage: <table> ["+FETCH_ARG+"<fetch family:fetch qualifier>] ["+FILTER_ARG+"<filter family:filter qualifier:filter value>] ["+NUM_ROWS_ARG+"<number of rows to print>]";
}


public static void main(String[] args)
{
	Integer numRows = null;

  try {
    if (args.length < 2)
    {
    	System.out.println(usage());
      	System.out.println("Note: hbase config must be in classpath");
    	return;
    }

    // First the setup work
    table = args[0];

    // Initialize query objects
    HBaseConfiguration config = new HBaseConfiguration();
    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists(table.getBytes())) {
      System.out.println("table does not exist: " + table);
      return;
    }

    HTable myTable = new HTable(config, table.getBytes());
    System.out.println("scanning full table:");
    Scan s = new Scan();

    // Parse user query values
    for (int i=1; i<args.length; ++i)
    {
    	if (args[i].startsWith(FETCH_ARG)) {
    		String val = args[i].substring(FETCH_ARG.length());
    		Column col = new Column(val);

    	    if (col.getQualifier()==null) {
            	System.out.println("setting fetch ("+col.getFamily()+")");
    	        s.addColumn(Bytes.toBytes(col.getFamily()));
    	    }
    	    else {
            	System.out.println("setting fetch ("+col.getFamily()+")("+col.getQualifier()+")");
    	    	s.addColumn(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getQualifier()));
    	    }
    	}
    	else if (args[i].startsWith(FILTER_ARG)) {
    		String val = args[i].substring(FETCH_ARG.length());
    		Column col = new Column(val);

    		if (col.getValue() == null) {
    			System.err.println("Invalid specification of filter");
    			System.err.println(usage());
    			return;
    		}
        	System.out.println("setting filter to ("+col.getFamily()+")("+col.getQualifier()+") value("+col.getValue()+")");
        	SingleColumnValueFilter filter = new SingleColumnValueFilter(
        			Bytes.toBytes(col.getFamily()),
        			Bytes.toBytes(col.getQualifier()),
        			CompareFilter.CompareOp.EQUAL,
        			Bytes.toBytes(col.getValue()));
        	s.setFilter(filter);
    	}
    	else if (args[i].startsWith(NUM_ROWS_ARG)) {
    		String val = args[i].substring(NUM_ROWS_ARG.length());
            numRows = Integer.parseInt(val);
    	}
    }

    ResultScanner scanner = myTable.getScanner(s);
    printRow(scanner, numRows, null);

    	// fin~
  }
  catch (MasterNotRunningException e) {
    e.printStackTrace();
  }
  catch (IOException e) {
    e.printStackTrace();
  }

	}

/**
 * Function available which loops over rows in scanner and simply counts them and prints final count
 * @param scanner
 * @throws IOException
 */
public static void countRows(ResultScanner scanner, Integer everyNumRows)
	throws IOException
{
	int rows = 0;
	for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
		rows++;
	    if (everyNumRows!=null && (rows % (everyNumRows.intValue()) == 0)) {
	    	System.out.print("..."+rows);
	    	System.out.flush();
	    }
	}
	System.out.println("\ntotal rows: " + rows);
	System.out.flush();
}

/**
 * Just a generic print function given an iterator. Not necessarily just for printing a single row
 *
 * @param scanner
 * @throws IOException
 */
public static void printRow(ResultScanner scanner, Integer numPrintRows, Integer everyNumRows)
    throws IOException
	{
		// iterates through and prints
  int rows = 0;
  for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
    rows++;

    boolean printIt = false;
    if (everyNumRows!=null) {
    	if( rows % (everyNumRows.intValue()) == 0) {
    		printIt = true;
    	}
    }
    else if (numPrintRows==null || (rows <= numPrintRows.intValue())) {
    	printIt = true;
    }
    if (printIt) {
    	System.out.println("**************************  R O W  ("+rows+")  **********************");
    	printOneRow(rr);

    	System.out.println("*********************************************************************");
    }
    if (numPrintRows != null && rows==numPrintRows.intValue())
    	return;
  }

  System.out.println("total rows: " + rows);
}

public static void printOneRow(Result rr)
{
  for (KeyValue kv : rr.list()) {
    String key = new String(rr.getRow());
    System.out.println(key + " -> " + new String(kv.getFamily()) + ":" + new String(kv.getQualifier()) + ":::"
        + new String(kv.getValue()));
  }
}


}
