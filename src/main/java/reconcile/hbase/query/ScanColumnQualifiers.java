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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ScanColumnQualifiers {

	private static String table = "example";

private static Set<String> columnFamily;


public static void main(String[] args)
	{
  try {
    if (args.length < 2)
    	{
      System.out.println("Usage: <table> [<column family>]+ ");
      System.out.println("Note: hbase config must be in classpath");
    		return;
    	}

    	// First the setup work
    table = args[0];
    columnFamily = Sets.newHashSet();
    for (int i = 1; i < args.length; i++) {
      columnFamily.add(args[i]);
    }

    HBaseConfiguration config = new HBaseConfiguration();
    HBaseAdmin admin = new HBaseAdmin(config);

    if (!admin.tableExists(table.getBytes())) {
      System.out.println("table does not exist: " + table);
      return;
    }

    HTable myTable = new HTable(config, table.getBytes());
    System.out.println("scanning full table:");
    Scan s = new Scan();
    for (String family : columnFamily) {
      s.addFamily(family.getBytes());
    }

    ResultScanner scanner = myTable.getScanner(s);
    printRow(scanner);

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
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
public static void printRow(ResultScanner scanner)
    throws IOException
	{
  Map<String, Long> quals = Maps.newHashMap();
		// iterates through and prints
  int rows = 0;
  for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
    rows++;
    for (KeyValue kv : rr.list()) {
      String column = new String(kv.getFamily()) + ":" + new String(kv.getQualifier());
      if (!quals.keySet().contains(column)) {
        System.out.println(column);
        quals.put(column, 1l);
      }
      else {
        quals.put(column, quals.get(column) + 1);
      }
    }
    // print out the row we found and the columns we were looking for

  }

  System.out.println("\n------------------------");
  System.out.println("total rows: " + rows);
  for (String key : quals.keySet()) {
    System.out.println(key + ": " + quals.get(key));
  }
	}


}
