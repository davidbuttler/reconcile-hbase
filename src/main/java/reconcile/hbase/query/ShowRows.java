/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by David Buttler, buttler1@llnl.gov CODE-400187 All rights reserved. This file is part of
 * RECONCILE
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License (as published by the Free Software Foundation) version 2, dated June 1991. This program is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full text see license.txt
 */
package reconcile.hbase.query;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;

import gov.llnl.text.util.Timer;


public class ShowRows {

private static String table = "example";

private static String columnFamily;

private static String columnQualifer;


public static void main(String[] args)
	{
  try {
    if (args.length != 3)
    	{
      System.out.println("Usage: <table> <column family> <column qualifier> ");
      System.out.println("Note: hbase config must be in classpath");
    		return;
    	}

    	// First the setup work
    table = args[0];
    columnFamily = args[1];
      columnQualifer = args[2];

    HBaseConfiguration config = new HBaseConfiguration();
    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists(table.getBytes())) {
      System.out.println("table does not exist: " + table);
      return;
    }

    HTable myTable = new HTable(config, table.getBytes());
    System.out.println("scanning full table:");
    Scan s = null;
    s = new Scan();
    s.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifer));
    System.out.println("setting filter to (" + columnFamily + ":" + columnQualifer + ") ");

    ResultScanner scanner = myTable.getScanner(s);
    countRows(scanner, Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifer));

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
 * Just a generic print function given an iterator. Not necessarily just for printing a single row
 *
 * @param scanner
 * @throws IOException
 */
public static void countRows(ResultScanner scanner, byte[] family, byte[] qualifier)
    throws IOException
	{
		// iterates through and prints
  int rows = 0;
  Timer t = new Timer();
  for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
    System.out.println(new String(rr.getRow(), Charsets.UTF_8) + ":"
        + new String(rr.getValue(family, qualifier), Charsets.UTF_8));
    t.increment();
    rows++;
  }
  System.out.println("total rows: " + rows);
  t.end();
	}


}
