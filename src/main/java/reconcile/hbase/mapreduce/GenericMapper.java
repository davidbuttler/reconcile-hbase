/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by David Buttler, buttler1@llnl.gov All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License (as published by the Free Software Foundation) version 2, dated June 1991. This program is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full text see license.txt
 *
 */
package reconcile.hbase.mapreduce;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

/**
 * Map/Reduce task for annotating Named Entities
 *
 * @author David Buttler
 *
 */
public class GenericMapper extends ChainableAnnotationJob {


/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>optional source argument to limit the rows to tag
 *          <li>-keyListFile=<hdfs file containing keys> optional argument to specify processing of only select rows
 *          </ol>
 */
public static void main(String[] args)
{
  try {
    ToolRunner.run(new Configuration(), new GenericMapper(), args);
  }
  catch (Exception e) {
    e.printStackTrace();
  }

}

private Class<? extends AnnotateMapper> mapperClass;

@SuppressWarnings("unchecked")
@Override
public void init(JobConfig jobConfig, Job job, Scan scan)
{
  Set<String> families = jobConfig.getArg("-family=");
  for (String family : families) {
    scan.addFamily(family.getBytes());
  }
  String mapper = jobConfig.getFirstArg("-mapper=");
  try {
    mapperClass = (Class<? extends AnnotateMapper>) Class.forName(mapper);
  }
  catch (ClassNotFoundException e) {
    throw new RuntimeException(e);
  }
}

@Override
public Class<? extends AnnotateMapper> getMapperClass()
{
  return mapperClass;
}



}
