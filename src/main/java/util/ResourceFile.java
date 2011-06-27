/*
 * Copyright (c) 2011, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by Teresa Cottom, cottom1@llnl.gov CODE-400187 All rights reserved. This file is part of
 * RECONCILE
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License (as published by the Free Software Foundation) version 2, dated June 1991. This program is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full text see license.txt
 */
package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.mortbay.log.Log;

public class ResourceFile {

public static final String HDFS_PREFIX = "hdfs:";

public static boolean isGzipFile(String fileName)
{
  return fileName.endsWith(".gz");
}

public static boolean isHDFSFile(String fileName)
{
  return fileName.startsWith(HDFS_PREFIX);
}

public static String hdfsFileName(String fileURI)
{
  return fileURI.substring(HDFS_PREFIX.length());
}

public static InputStream getInputStream(String fileURI)
    throws IOException
{
  if (isHDFSFile(fileURI)) {
    // Meta data file is a file within HDFS
    Configuration conf = new Configuration();
    DFSClient dfsClient = new DFSClient(conf);
    String hdfsFileName = hdfsFileName(fileURI);

    if (!dfsClient.exists(hdfsFileName)) return null;

    Log.info("Opening HDFS file (" + hdfsFileName + ")");
    return dfsClient.open(hdfsFileName);
  }
  else {
    File metaFile = new File(fileURI);
    if (metaFile.exists()) {
      // Meta data file is a file on local file system
      Log.info("Opening local file (" + metaFile + ")");
      return new FileInputStream(metaFile);
    }
    else {
      // Try meta data file as resource available in jar
      try {
        throw new IOException("foo");
      }
      catch (IOException e) {
        String className = e.getStackTrace()[1].getClassName();
        try {
          Class<?> callerClass = Class.forName(className);
          Log.info("Opening resource file (" + fileURI + ")");
          Log.info("using class (" + className + ")");
          return callerClass.getResourceAsStream(fileURI);
        }
        catch (ClassNotFoundException e1) {
          e1.printStackTrace();
          throw new RuntimeException(e1);
        }
      }
    }
  }
}

public static OutputStream getOutputStream(String fileURI)
    throws IOException
{
  OutputStream os = null;

  if (isHDFSFile(fileURI)) {
    // Meta data file is a file within HDFS
    Configuration conf = new Configuration();
    DFSClient dfsClient = new DFSClient(conf);
    String hdfsFileName = hdfsFileName(fileURI);

    // Create file in HDFS
    os = dfsClient.create(hdfsFileName, true);
  }
  else {
    // Create file on the local file system
    File metaFile = new File(fileURI);
    os = new FileOutputStream(metaFile);
  }
  // Create a GZIP File if 'requested'
  if (isGzipFile(fileURI)) {
    os = new GZIPOutputStream(os);
  }
  return os;
}

}
