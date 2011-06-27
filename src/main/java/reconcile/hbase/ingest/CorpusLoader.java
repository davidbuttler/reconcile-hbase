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

import static reconcile.hbase.table.DocSchema.annotationsCF;
import static reconcile.hbase.table.DocSchema.metaCF;
import static reconcile.hbase.table.DocSchema.srcCF;
import static reconcile.hbase.table.DocSchema.srcId;
import static reconcile.hbase.table.DocSchema.srcName;
import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Put;

import gov.llnl.text.util.Timer;

import reconcile.data.AnnotationSet;
import reconcile.data.AnnotationWriterBytespan;
import reconcile.data.Corpus;
import reconcile.data.CorpusFile;
import reconcile.data.Document;
import reconcile.hbase.table.DocSchema;

/**
 * Load documents in the reconcile corpus format into hbase
 * <p>
 * args:
 * <ol>
 * <li>source</li>
 * <li>input directory</li>
 * </ol>
 *
 * @author buttler1
 *
 */
public class CorpusLoader {

public static void main(String[] args)
    throws IOException
{
  if (args.length != 2) {
    System.out.println("Usage: <source name> <input dir>");
    return;
  }

  try {
    String source = args[0];
    File inputDir = new File(args[1]);

    DocSchema docTable = new DocSchema(source, true);

    AnnotationWriterBytespan writer = new AnnotationWriterBytespan();
    Timer t = new Timer(1);
    Corpus corpus = new CorpusFile(inputDir);
    for (Document d : corpus) {
      t.increment();
      String id = source + ":" + d.getDocumentId();
      String rowKey = DigestUtils.shaHex(id);
      Put mut = new Put(rowKey.getBytes());
      mut.add(srcCF.getBytes(), srcName.getBytes(), source.getBytes());
      mut.add(srcCF.getBytes(), srcId.getBytes(), id.getBytes());
      mut.add(textCF.getBytes(), textRaw.getBytes(), d.getText().getBytes());

      for (String anSetName : d.getAnnotationSetNames()) {
        AnnotationSet anSet = d.getAnnotationSet(anSetName);
        StringWriter sw = new StringWriter();
        PrintWriter out = new PrintWriter(sw);
        writer.write(anSet, out);
        out.flush();
        mut.add(annotationsCF.getBytes(), anSetName.getBytes(), sw.toString().getBytes());
      }
      for (String key : d.getMetaDataKeys()) {
        String val = d.getMetaData(key);
        mut.add(metaCF.getBytes(), key.getBytes(), val.getBytes());
      }

      docTable.put(mut);
      if (t.getCount() % 100 == 0) {
        docTable.flushCommits();
      }

    }
    docTable.flushCommits();
    docTable.close();
  }
  catch (Exception e) {
    e.printStackTrace();
  }
}
}
