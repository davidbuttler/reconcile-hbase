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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.collect.Maps;

import gov.llnl.text.util.MapUtil;
import gov.llnl.text.util.Timer;

import reconcile.hbase.table.DocSchema;

public class MeasureQualifierSizeDistribution {

private static String tableName = "example";

private static String family;

private static String qual;

	public static void main(String[] args)
	{
  try {
    if (args.length != 4)
    	{
      System.out.println("Usage: <table> <family> <qualifier> <tick>");
      System.out.println("Note: hbase config must be in classpath");
    		return;
    	}

    	// First the setup work
    tableName = args[0];
    family = args[1];
    qual = args[2];
    int tick = Integer.parseInt(args[3]);

    Configuration conf = HBaseConfiguration.create();
    HTable table = new HTable(conf, tableName.getBytes());

    System.out.println("scanning full table:");
    Scan scan = new Scan();
    scan.addFamily(family.getBytes());

    ResultScanner scanner = table.getScanner(scan);
    printRow(scanner, tick);
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
public static void printRow(ResultScanner scanner, int tick)
    throws IOException
{
  // iterates through and prints
  int rows = 0;
  Map<Integer, Integer> mbMap = Maps.newTreeMap();
  Map<Integer, Integer> kbMap = Maps.newTreeMap();

  Timer t = new Timer(tick);
  for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
    t.increment();
    rows++;
    String s = DocSchema.getColumn(rr, family, qual);
    int kb = MeasureRowSizeDistribution.getKB(s.length());
    int mb = MeasureRowSizeDistribution.getMB(s.length());
    MapUtil.addCount(kbMap, kb);
    MapUtil.addCount(mbMap, mb);
    // print out the row we found and the columns we were looking for
  }

  System.out.println("mb size:");
  for (int k : mbMap.keySet()) {
    System.out.println(k + ": " + mbMap.get(k));
  }

  System.out.println("kb size:");
  for (int k : kbMap.keySet()) {
    System.out.println(k + ": " + kbMap.get(k));
  }
  System.out.println("total rows: " + rows);
  t.end();

}
}
