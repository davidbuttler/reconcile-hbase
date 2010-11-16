/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
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
package reconcile.hbase.mapreduce;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import reconcile.hbase.table.DocSchema;

public class KeyListInputFormat extends FileInputFormat<ImmutableBytesWritable, Result>
{
	static final Log LOG = LogFactory.getLog(KeyListInputFormat.class);

	static final String TOP = "trinidad.hbase.mapreduce.KeyListInputFormat";
	static final public String SCAN_FAMILIES = TOP+".scanFamilies";
	static final public String LOG_ON = TOP+".logOn";

	private static class KeyRowReader extends RecordReader<ImmutableBytesWritable, Result>
	{
	    private LineRecordReader lrr ;
	    private Result result;
	    private ImmutableBytesWritable key;

private DocSchema table;
	    private boolean logOn = false;
	    private TreeSet<String> scanFamilies = new TreeSet<String>();

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException
		{
  table = new DocSchema(arg1.getConfiguration().get(JobConfig.SOURCE_NAME));

			lrr = new LineRecordReader();
			lrr.initialize(arg0, arg1);

			// Set the families retrieved by scan
			String value = arg1.getConfiguration().get(SCAN_FAMILIES);
			if (value!=null) {
				for (String family : StringUtils.split(value)) {
					scanFamilies.add(family);
				}
			}

			// Enable / disable logging
			value = arg1.getConfiguration().get(LOG_ON);
			if (value!=null) {
        logOn = Boolean.parseBoolean(value);
      }
		}

	    @Override
      public ImmutableBytesWritable getCurrentKey()
	    {
	    	return key;
	    }

	    @Override
      public Result getCurrentValue()
	    {
	    	return result;
	    }

	    @Override
      public boolean nextKeyValue()
	    	throws IOException
	    {
	    	result = null;
	    	key = null;

	    	while (result == null && lrr.nextKeyValue())
	    	{
	    		Text line = lrr.getCurrentValue();
	    		String stringKey = Bytes.toString(line.getBytes());
	    		if (logOn) {
            LOG.info("Search for record with key:"+stringKey);
          }

	    		byte[] byteKey = Bytes.toBytes(stringKey);
	    		key = new ImmutableBytesWritable(byteKey);
	    		Get get = new Get(byteKey);

	    		//get.addFamily(DocSchema.srcCF.getBytes());
	    		//get.addFamily(DocSchema.textCF.getBytes());
	    		//get.addFamily(DocSchema.elecentCF.getBytes());

	    		// Add scan families
	    		for (String family : scanFamilies) {
	    			if (logOn) {
              LOG.info("... add get family("+family+")");
            }
	    			get.addFamily(family.getBytes());
	    		}

	    		result = table.get(get);

	    		if (logOn) {
	    			if (result==null) {
              LOG.info("Unable to find record:"+stringKey);
            }
            else {
              LOG.info("Loaded record:"+stringKey);
            }
	    		}
	    	}
	    	return result!=null;
	    }

	    @Override
      public float getProgress()
	    {
	      return lrr.getProgress() ;
	    }

	    @Override
      public synchronized void close() throws IOException {
	      lrr.close();
	    }
	}


	@Override
	public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException
	{
		KeyRowReader reader = new KeyRowReader();
		reader.initialize(arg0, arg1);
		return reader;
	}

}
