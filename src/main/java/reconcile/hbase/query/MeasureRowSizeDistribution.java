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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Maps;

import gov.llnl.text.util.MapUtil;
import gov.llnl.text.util.Timer;

public class MeasureRowSizeDistribution {

private static String tableName = "example";

	public static void main(String[] args)
	{
  try {
    if (args.length != 2)
    	{
      System.out.println("Usage: <table> <tick>");
      System.out.println("Note: hbase config must be in classpath");
    		return;
    	}

    	// First the setup work
    tableName = args[0];
    int tick = Integer.parseInt(args[1]);

    Configuration conf = HBaseConfiguration.create();
    HTable table = new HTable(conf, tableName.getBytes());

    System.out.println("scanning full table:");
    ResultScanner scanner = table.getScanner(new Scan());
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
    ImmutableBytesWritable bytes = rr.getBytes();
    byte[] buf = bytes.get();
    int offset = bytes.getOffset();
    int kb = getKB(buf.length - offset);
    int mb = getMB(buf.length - offset);
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

public static int getMB(int length)
{
  int mbLength = length >> 10;
  return getKB(mbLength);
}

public static int getKB(int length)
{
  int kb = length >> 10;
  if (kb == 0)
    return 0;
  else if (kb == 1)
    return 1;
  else if (kb == 2)
    return 2;
  else if (kb == 3)
    return 3;
  else if (kb == 4)
    return 4;
  else if (kb == 5)
    return 5;
  else if (kb == 6)
    return 6;
  else if (kb == 7)
    return 7;
  else if (kb == 8)
    return 8;
  else if (kb == 9)
    return 9;
  else if (kb < 20)
    return 10;
  else if (kb < 30)
    return 20;
  else if (kb < 40)
    return 30;
  else if (kb < 50)
    return 40;
  else if (kb < 60)
    return 50;
  else if (kb < 70)
    return 60;
  else if (kb < 80)
    return 70;
  else if (kb < 90)
    return 80;
  else if (kb < 100) return 90;

  return 100;
}
}
