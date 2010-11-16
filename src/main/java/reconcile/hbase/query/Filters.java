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
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import gov.llnl.text.util.FileUtils;

import reconcile.hbase.table.DocSchema;

public class Filters {

	/**
	 * Add filter to scan object which will return rows with keys that
	 * are specified in the given HDFS file [one per line]
	 *
	 * @param scan
	 * @param hdfsKeyListFile
	 * @throws IOException
	 */
	public static void keys(Scan scan, String hdfsKeyListFile) throws IOException
	{
		HBaseConfiguration conf = new HBaseConfiguration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream is = fs.open(new Path(hdfsKeyListFile));

		List<String> keyList = FileUtils.readFileLines(is);
		keys(scan, keyList);
	}

	/**
	 * Add filter to scan object which will return rows with given collection of keys.
	 *
	 * @param scan
	 * @param keys
	 */
	public static void keys(Scan scan, Collection<String> keys)
	{
		// Add in list of keys filter
		FilterList listFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		for (String key : keys) {
			RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(key.getBytes()));
			listFilter.addFilter(rowFilter);
		}
		FilterList topFilter = listFilter;

		// Add any previous filter
		Filter filter = scan.getFilter();
		if (filter!=null) {
			topFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			topFilter.addFilter(filter);
			topFilter.addFilter(listFilter);
		}

		// Set filter
		scan.setFilter(topFilter);
	}

	public static void ingestDate(Scan scan, Date begin) throws IOException
	{
		addDateRange(scan,
				DocSchema.metaCF, DocSchema.ingestDate,
				begin, null);
	}

	public static void addDateRange(Scan scan,
			String family, String qualifier,
			Date begin, Date end) throws IOException
	{
		if (begin==null && end==null)
			throw new IOException("must specify at least an end or begin date if not both");

		FilterList listFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);

		Filter filter = scan.getFilter();
		if (filter!=null) {
			listFilter.addFilter(filter);
		}

		if (begin!=null) {
			// Add begin date filter if provided
			String beginValue = DocSchema.formatSolrDate(begin);

			SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
					family.getBytes(), qualifier.getBytes(),
					CompareFilter.CompareOp.GREATER_OR_EQUAL, beginValue.getBytes());

			listFilter.addFilter(valueFilter);
		}

		if (end!=null) {
			// Add end date filter if provided
			String endValue = DocSchema.formatSolrDate(end);

			SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
					family.getBytes(), qualifier.getBytes(),
					CompareFilter.CompareOp.LESS_OR_EQUAL, endValue.getBytes());

			listFilter.addFilter(valueFilter);
		}

		scan.setFilter(listFilter);
	}
}
