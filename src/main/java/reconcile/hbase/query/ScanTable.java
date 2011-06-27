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

import static reconcile.hbase.query.ScanColumn.printRow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class ScanTable {

private static String tableName = "example";

	public static void main(String[] args)
	{
  try {
    if (args.length != 1)
    	{
      System.out.println("Usage: <table>");
      System.out.println("Note: hbase config must be in classpath");
    		return;
    	}

    	// First the setup work
    tableName = args[0];

    Configuration conf = HBaseConfiguration.create();
    HTable table = new HTable(conf, tableName.getBytes());

    System.out.println("scanning full table:");
    ResultScanner scanner = table.getScanner(new Scan());
    printRow(scanner, null, null);
  }
  catch (IOException e) {
    e.printStackTrace();
  }

		// fin~

	}


}
