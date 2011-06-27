/*
 * Copyright (c) 2010, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by Teresa Cottom, cottom1@llnl.gov CODE-400187 All rights reserved. This file is part of
 * RECONCILE
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public
 * License (as published by the Free Software Foundation) version 2, dated June 1991. This program is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA For full text see license.txt
 */package reconcile.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import reconcile.hbase.table.DocSchema;

/**
 * Abstract base mapper which initializes source, key list, and HTable for
 * working with the DocSchema as input and/or output.
 *
 * @author cottom1
 *
 * @param <ReducerType>
 */
public abstract class DocMapper<ReducerType> extends TableMapper<ImmutableBytesWritable, ReducerType>
{
	private String source;
	private String keyList;
	private String table;
	protected DocSchema docTable;

	@Override
	public void setup(Context context)
	    throws IOException, InterruptedException
	{
	  super.setup(context);
	  source = context.getConfiguration().get(JobConfig.SOURCE_CONF);

	  table = context.getConfiguration().get(JobConfig.TABLE_CONF);
	  if (table == null || table.trim().length() == 0) {
	    table = source;
	  }

	  docTable = new DocSchema(table);

	  keyList = context.getConfiguration().get(JobConfig.KEY_LIST_CONF);
	}

@Override
public void cleanup(Context context)
    throws IOException, InterruptedException
{
  try {
    docTable.flushCommits();
    docTable.close();
  }
  catch (IOException e) {
    context.getCounter(contextHeader(), "cleanup error: " + e.getMessage()).increment(1);
    e.printStackTrace();
  }

}
	public String getSourceName()
	{
	  return source;
	}

	public String getKeyList()
	{
	  return keyList;
	}

	public String getTableName()
	{
	  return table;
	}

	public String contextHeader()
	{
	  return getClass().getSimpleName();
	}

protected boolean isRowSizeTooLarge(Result row, Context context, int maxRowLength)
  {
    int size = 0;
    KeyValue[] kv = row.raw();
    for (KeyValue k : kv) {
      size += k.getLength();
    }
  long logSize = (int) Math.round(Math.log10(size));
  context.getCounter(contextHeader() + " row size log10", String.valueOf(logSize)).increment(1);
  // System.out.println("row size: " + size);
  if (size > maxRowLength) {
    context.getCounter(contextHeader(), "skip -- row too large (" + maxRowLength + ")").increment(1);
      return true;

    }
    return false;
  }

protected boolean isRawTextInvalidSize(String rawText, Context context, int maxRawTextLength)
  {
    if (rawText == null || rawText.length() == 0) {
    context.getCounter(contextHeader(), "skip -- no raw text").increment(1);
      return true;
    }
  if (rawText.length() > maxRawTextLength) {
    context.getCounter(contextHeader(), "skip -- raw text too long (" + maxRawTextLength + ")")
          .increment(1);
      return true;
    }
    return false;
  }

}
