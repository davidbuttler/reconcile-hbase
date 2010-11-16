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
package reconcile.hbase.mapreduce.annotation;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

import reconcile.data.AnnotationSet;
import reconcile.data.AnnotationWriterBytespan;


public class AnnotationUtils {

public static final Pattern pSpace = Pattern.compile("\\s+");

  public static String getAnnotationStr(AnnotationSet annSet)
  {
    AnnotationWriterBytespan writer = new AnnotationWriterBytespan();
    StringWriter sw = new StringWriter();
    PrintWriter out = new PrintWriter(sw, true);
    writer.write(annSet, out);
    out.flush();
    out.close();
    String annotationStr = sw.toString();
    return annotationStr;
  }

/**
 * @return
 */
public static String toString(Collection<String> array)
{
  if (array == null) return null;
  StringBuilder b = new StringBuilder();
  List<String> sortedArray = Lists.newArrayList(array);
  Collections.sort(sortedArray);
  for (String item : array) {
    b.append(pSpace.matcher(item).replaceAll(" "));
    b.append("\t");
  }
  return b.toString().trim();

}

/**
 * @param coutries
 * @return
 */
public static String toString(String[] array)
{
  if (array == null) return null;
  StringBuilder b = new StringBuilder();
  for (String item : array) {
    b.append(pSpace.matcher(item).replaceAll(" "));
    b.append("\t");
  }
  return b.toString().trim();
}

}
