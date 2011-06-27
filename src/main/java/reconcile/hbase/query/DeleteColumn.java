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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteColumn {

private static String tableName = "example";

private static String columnFamily;

private static String columnQualifer;

private static String filterFamily;

private static String filterColumn;

private static String filterValue;

public static void main(String[] args)
{
  try {
    if (args.length != 2 && args.length != 3 && args.length != 5) {
      System.out
          .println("Usage: <table> <column family> [<column qualifier>] [<filter family> <filter column> <filter value>]");
      System.out.println("Note: hbase config must be in classpath");
      return;
    }

    // First the setup work
    tableName = args[0];
    columnFamily = args[1];
    if (args.length == 3) {
      columnQualifer = args[2];
    }
    else if (args.length == 5) {
      filterFamily = args[2];
      filterColumn = args[3];
      filterValue = args[4];
    }

    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (!admin.tableExists(tableName.getBytes())) {
      System.out.println("table does not exist: " + tableName);
      return;
    }

    HTable table = new HTable(conf, tableName.getBytes());

    System.out.println("scanning full table:");
    Scan s = null;
    if (columnQualifer == null) {
      s = new Scan();
      s.addFamily(Bytes.toBytes(columnFamily));
    }
    else {
      s = new Scan();
      s.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifer));
    }
    if (filterValue != null) {
      System.out.println("setting filter to (" + filterFamily + ":" + filterColumn + ") value(" + filterValue + ")");
      SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(filterFamily), Bytes
          .toBytes(filterColumn), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(filterValue));
      s.setFilter(filter);
    }

    ResultScanner scanner = table.getScanner(s);
    delete(table, scanner);
  }
  catch (IOException e) {
    e.printStackTrace();
  }

  // fin~

}

/**
 * Just a generic print function given an iterator. Not necessarily just for printing a single row
 *
 * @param scanner
 * @throws IOException
 */
public static void delete(HTable table, ResultScanner scanner)
    throws IOException
{
  // iterates through and prints
  int rows = 0;
  int deleted = 0;
  for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
    rows++;
    byte[] row = rr.getRow();
    String s = new String(row);
    Delete delete = new Delete(row);
    if (columnQualifer == null) {
      delete.deleteFamily(columnFamily.getBytes());
    }
    else {
      delete.deleteColumn(columnFamily.getBytes(), columnQualifer.getBytes());
    }
    table.delete(delete);
    System.out.println("delete: " + s);
    deleted++;

  }

  System.out.println("total rows: " + rows);
  System.out.println("deleted rows: " + deleted);
}

}
