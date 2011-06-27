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
 */
package reconcile.hbase.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import reconcile.hbase.table.DocSchema;

/**
 * Parses and tracks command-line arguments for standard DocSchema parameters,
 * and can initialize for a typical Mapper/Reducer job which gets and/or
 * puts to the DocSchema
 *
 * @author cottom1
 *
 */
public class JobConfig {

/**
 * Command-line option to specify source name to limit rows which are processed to those with given source name
 */
static public final String SOURCE_ARG = "-source=";
/**
 * Command-line option to specify the table name to use in HBase [default is table=source]
 */
static public final String TABLE_ARG = "-table=";
/**
 * Command-line option to specify the start row to use in HBase [default is none]
 */
static public final String START_ROW_ARG = "-startRow=";
/**
 * Command-line option to specify the stop row to use in HBase [default is none]
 */
static public final String STOP_ROW_ARG = "-stopRow=";
/**
 * Command-line option to specify HDFS file containing list of keys of rows to process [1 key per line]
 */
static public final String KEY_LIST_ARG = "-keyList=";
/**
 * Command-line option to specify to only retrieve columns with time stamp in given range
 */
static public final String TIME_RANGE_ARG = "-timeRange=";
/**
 * Command-line option to specify to only retrieve columns for given time stamp
 */
static public final String TIME_STAMP_ARG = "-timeStamp=";

/**
 * Configuration variables set on Job context
 */
static final String BASE = "trinidad.hbase.mapreduce.JobConfig.Mapper";
public static final String TABLE_CONF = BASE+".table";
public static final String SOURCE_CONF = BASE+".source";
public static final String KEY_LIST_CONF = BASE+".keyList";
public static final String START_ROW_CONF = BASE+".startRow";
public static final String STOP_ROW_CONF = BASE+".stopRow";
public static final String START_TIME_CONF = BASE+".startTime";
public static final String STOP_TIME_CONF = BASE+".stopTime";
public static final String TIME_STAMP_CONF = BASE+".timeStamp";

private String[] args = null;
private String table = null;
private String source = null;
private String keyListFile = null;
private String startRow = null;
private String stopRow = null;
private Long startTime = null;
private Long stopTime = null;
private Long timeStamp = null;

private StringBuffer argString = new StringBuffer();

/**
 * Return a string containing the 'usage' supported arguments.
 * @return
 */
public static String usage()
{
  return "[ " + SOURCE_ARG + "<source name> | " + KEY_LIST_ARG + "<HDFS key list file> | " + TABLE_ARG
      + "<table name> | "+START_ROW_CONF+"<row key> | "+STOP_ROW_CONF+"<row key> "
      + TIME_RANGE_ARG+"<begintime,endtime> | "+TIME_STAMP_ARG+"<time> "
      + "]";
}

/**
 *
 * @param args
 *          :
 *          <ol>
 *          <li>-table='schema table name' - optional argument to specify name of table [defaults to source]
 *          <li>-source='source name' - optional argument to specify processing of only rows with given source name
 *          <li>-keyList='hdfs key list file' - optional argument to specify processing of only select rows by key
 *          <li>-startRow='row key' - optional argument to specify to start processing at the given row key
 *          <li>-stopRow='row key' - optional argument to specify to stop processing at the given row key
 *          <li>-timeRanage='startTime,endTime' - optional argument to process only columns with time stamps within given time range
 *          <li>-timeStamp='time' - optional argument to process only columns with given time stamp
 *          </ol>
 */
public JobConfig(String[] args)
{
	this.args = args;
	for (String arg : args)
	{
		if (arg.startsWith(SOURCE_ARG)) {
			String value = arg.substring(SOURCE_ARG.length());
			if (!value.startsWith("$")) {
				source = value;
			}
		}
		else if (arg.startsWith(TABLE_ARG)) {
			String value = arg.substring(TABLE_ARG.length());
			if (!value.startsWith("$")) {
				table = value;
			}
		}
		else if (arg.startsWith(KEY_LIST_ARG)) {
			String value = arg.substring(KEY_LIST_ARG.length());
			if (!value.startsWith("$")) {
				keyListFile = value;
			}
		}
		else if (arg.startsWith(START_ROW_ARG)) {
			String value = arg.substring(START_ROW_ARG.length());
			if (!value.startsWith("$") && value.length() > 0) {
				startRow = value;
			}
		}
		else if (arg.startsWith(STOP_ROW_ARG)) {
			String value = arg.substring(STOP_ROW_ARG.length());
			if (!value.startsWith("$") && value.length() > 0) {
				stopRow = value;
			}
		}
		else if (arg.startsWith(TIME_RANGE_ARG)) {
			String value = arg.substring(TIME_RANGE_ARG.length());
			if (!value.startsWith("$") && value.length() > 0) {
				String vals[] = value.split(",");
				if (vals.length==2) {
					startTime = getTimeArg(vals[0]);
					stopTime  = getTimeArg(vals[1]);
				}
				else {
					throw new RuntimeException("Time range argument value is not correct.  Should be <startTime>,<endTime>");
				}
			}
		}
		else if (arg.startsWith(TIME_STAMP_ARG)) {
			String value = arg.substring(TIME_STAMP_ARG.length());
			if (!value.startsWith("$") && value.length() > 0) {
				timeStamp = getTimeArg(value);
			}
		}
	}
	if (table == null) {
		table = source;
	}

	// Create a single string containing all command line arguments
	for (String arg : args) {
		argString.append(arg + ",");
	}
}

/**
 * Method to properly initialize the Mapper/Reducer jobs based on options
 *
 * @param LOG
 * @param job
 * @param scan
 * @param mapClass
 * @throws IOException
 */
public void initTableMapperNoReducer(Log LOG, Job job, Scan scan, Class<? extends DocMapper<?>> mapClass)
    throws IOException
{
  Preconditions.checkNotNull(scan);
	job.setJobName(job.getJobName() + "(" + argString + ")");
	if (source != null) {
		job.getConfiguration().set(SOURCE_CONF, source);
	}
	if (table != null) {
		job.getConfiguration().set(TABLE_CONF, table);
	}
	if (keyListFile != null) {
		job.getConfiguration().set(KEY_LIST_CONF, keyListFile);
	}
	if (startRow != null) {
		job.getConfiguration().set(START_ROW_CONF, startRow);
		if (scan!=null) {
			scan.setStartRow(startRow.getBytes());
		}
	}
	if (stopRow != null) {
		job.getConfiguration().set(STOP_ROW_CONF, stopRow);
		if (scan!=null) {
			scan.setStopRow(stopRow.getBytes());
		}
	}
	if (timeStamp != null) 
	{
		job.getConfiguration().set(TIME_STAMP_CONF, timeStamp.toString());		
		if (scan!=null)	
			scan.setTimeStamp(timeStamp.longValue());
	}
	if (startTime != null && stopTime!=null) 
	{
		LOG.info("Setting startTime("+startTime+") stopTime("+stopTime+")");
		job.getConfiguration().set(START_TIME_CONF, startTime.toString());		
		job.getConfiguration().set(STOP_TIME_CONF, stopTime.toString());		
		if (scan!=null)
			scan.setTimeRange(startTime.longValue(), stopTime.longValue());
	}
	
	if (keyListFile == null) {
		LOG.info("Setting scan for source name(" + source + ") table(" + table + ")");
		initTableMapperForSource(job, scan, mapClass);
	}
	else {
		LOG.info("Operating solely on keys in HDFS file (" + keyListFile + ") source(" + source + ") table(" + table + ")");
		initTableMapperForKeyList(job, mapClass);
	}

	StringBuffer families = new StringBuffer();
	if (scan.getFamilies() != null) {
		for (byte[] family : scan.getFamilies()) {
			families.append(Bytes.toString(family) + " ");
		}
	}
	StringBuffer other = new StringBuffer();
	if (scan.getFamilyMap() != null) {
		Map<byte[], NavigableSet<byte[]>> map = scan.getFamilyMap();
		if (map != null) {
			for (byte[] family : map.keySet()) {
				if (family == null) {
					continue;
				}
				other.append(Bytes.toString(family) + "[");
				if (map.get(family) != null) {
					for (byte[] qual : map.get(family)) {
						other.append(Bytes.toString(qual) + ",");
					}
				}
				other.append("]; ");
			}
		}
	}

	String familiesValue = families.toString();
	LOG.info("Setting scan retrieve families to (" + familiesValue + ")");
	LOG.info("Setting scan retrieve families/quals to (" + other.toString() + ")");
	job.getConfiguration().set(KeyListInputFormat.SCAN_FAMILIES, familiesValue);

	TableMapReduceUtil.initTableReducerJob(getTableName(), IdentityTableReducer.class, job);
	job.setNumReduceTasks(0);
}

/**
 * Method to initialize a mapper job which will operate on rows in 'doc' table for those matching the given source name,
 * or if source is null, all rows.
 *
 * @param job
 * @param scan
 * @param mapClass
 * @throws IOException
 */
private void initTableMapperForSource(Job job, Scan scan, Class<? extends DocMapper<?>> mapClass)
    throws IOException
{
	if (source != null) {
		scan.addColumn(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes());
		scan.setFilter(new SingleColumnValueFilter(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes(),
				CompareOp.EQUAL, source.getBytes()));
	}

	// set up mapper jobs
	TableMapReduceUtil.initTableMapperJob(getTableName(), scan, mapClass, ImmutableBytesWritable.class, Put.class, job);
}

/**
 * Method to initialize a mapper job which will operate only on rows in 'doc' table for keys listed in an HDFS file
 *
 * @param job
 * @param mapClass
 * @throws IOException
 */
private void initTableMapperForKeyList(Job job, Class<? extends DocMapper<?>> mapClass)
    throws IOException
{
	Path inputPath = new Path(keyListFile);
	job.setMapperClass(mapClass);
	job.setInputFormatClass(KeyListInputFormat.class);
	job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	job.setMapOutputValueClass(Put.class);
	FileInputFormat.addInputPath(job, inputPath);
}

/**
 * Return the name of the schema table as set by command line arguments [defaulted to source name]
 */
public String getTableName()
{
	return table;
}

/**
 * Return the source name as set by command line arguments
 *
 * @return
 */
public String getSource()
{
	return source;
}

/**
 * Return the key list file name as set by the command line arguments
 *
 * @return
 */
public String getKeyListFile()
{
	return keyListFile;
}

/**
 * Return the start row as set via command line arguments
 */
public String getStartRow()
{
	return startRow;
}

/**
 * Return the stop row as set via command line arguments
 */
public String getStopRow()
{
	return stopRow;
}

/**
 * Return the start time stamp for column query set via command line arguments
 * @return
 */
public Long getStartTime()
{
	return startTime;
}

/**
 * Return the stop time stamp for column query set via command line arguments
 * @return
 */
public Long getStopTime()
{
	return stopTime;
}

/**
 * Return the time stamp for column query set via command line arguments
 * @return
 */
public Long getTimeStamp()
{
	return timeStamp;
}

/**
 * Return the command-line arguments
 */
public String[] getArgs()
{
	return args;
}

/**
 * Return a set of argument values which match the given argument.  If there are
 * no matches, the returned set will be empty.
 * 
 * @param prefix
 * @return
 */
public Set<String> getArg(String prefix)
{
  Set<String> result = Sets.newTreeSet();
  for (String a : args) {
    if (a.startsWith(prefix)) {
      result.add(a.substring(prefix.length()));
    }
  }
  return result;
}

/**
 * Return the first argument value which matches given argument.  If argument is
 * not found or begins with unset ant variable indicator ($), then return null.
 *  
 * @param prefix
 * @return
 */
public String getFirstArg(String prefix)
{
	for (String a : args) {
		if (a.startsWith(prefix)) {
			String value = a.substring(prefix.length()).trim();
			if (value.length()>0)
				if (! value.startsWith("$"))
					return value;
		}
	}
	return null;
}

static SimpleDateFormat[] formats = { 
		new SimpleDateFormat("yyyy"),
		new SimpleDateFormat("yyyy-MM"),
		new SimpleDateFormat("yyyy-MM-dd"),
		new SimpleDateFormat("yyyy-MM-dd-HH"),
		new SimpleDateFormat("yyyy-MM-dd-HH-mm"),
		new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
};

/**
 * Get the time in milliseconds from the given string value time based arg.  Value
 * can be in milliseconds already, or above listed formats.
 * @param value
 * @return
 */
public static Long getTimeArg(String value) 
{
	try {
		String[] vals = value.split("-");
		if (vals.length > 1) {
			return formats[vals.length-1].parse(value).getTime();
		}
		if (value.length()==4) {
			return formats[0].parse(value).getTime();
		}
	}
	catch (ParseException e) {
		e.printStackTrace();
		return null;
	}
	
	// Simple long based time stamp in millis
	return Long.parseLong(value);
}

}
