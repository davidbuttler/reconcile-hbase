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
package reconcile.hbase.ingest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Put;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import gov.llnl.text.util.FileUtils;
import gov.llnl.text.util.RecursiveFileIterable;
import gov.llnl.text.util.Timer;

import reconcile.hbase.query.ExtensionFileFilter;
import reconcile.hbase.table.DocSchema;


/**
 * Load text files into the doc table
 *
 * @author buttler1
 *
 */
public class DocLoader {


public static void main(String[] args)
    throws IOException
{
  if (args.length != 2) {
    System.out.println("Usage: <source name> <input dir>");
    System.out.println("Note: hbase config must be in classpath");
    return;
  }

  try {
    String sourceName = args[0];
    byte[] source = sourceName.getBytes();
    File inputDir = new File(args[1]);

    DocSchema loader = new DocSchema(sourceName, true);

    Timer t = new Timer(1);
    for (File f : RecursiveFileIterable.iterate(inputDir, new ExtensionFileFilter("txt"))) {
      System.out.println("File: " + f.getAbsolutePath());
      if (f.isDirectory()) {
        continue;
      }
      t.increment();
      String id = getRelativePath(inputDir, f);
      Put mut = new Put(id.getBytes());
      mut.add(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes(), source);
      mut.add(DocSchema.srcCF.getBytes(), DocSchema.srcId.getBytes(), id.getBytes());
      mut.add(DocSchema.textCF.getBytes(), DocSchema.textOrig.getBytes(), FileUtils.readFile(f).getBytes());

      mut.add(DocSchema.metaCF.getBytes(), "fullPath".getBytes(), f.getAbsolutePath().getBytes());

        // And add the mutations
      loader.put(mut);

        if (t.getCount() % 100 == 0) {
        loader.flushCommits();
        }

      }
    loader.flushCommits();


    loader.flushCommits();
    loader.close();
  }
  catch (TableExistsException e) {
    e.printStackTrace();
  }
  catch (TableNotFoundException e) {
    e.printStackTrace();
  }

}


private static String getRelativePath(File inputDir, File f)
{
  Stack<String> pathStack = FileUtils.getRelativePath(inputDir, f);
  List<String> path = Lists.newArrayList();
  for (String s : pathStack) {
    path.add(0, s);
  }
  String p = Joiner.on("/").join(path);
  return p;
}
}
