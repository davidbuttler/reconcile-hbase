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
package reconcile.hbase.ingest.rss;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.xml.sax.SAXException;

import reconcile.hbase.mapreduce.JobConfig;
import util.URLData;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.syndication.io.FeedException;

public class RSSFeedMonitor 
{

public static final String URL_ARG ="-url=";
public static final String SAVE_ARG="-save=";

public static void main(String[] args) 
throws IOException, IllegalArgumentException, FeedException, SAXException
{	
	RSSFeedMonitor monitor = new RSSFeedMonitor(args);	
	monitor.downloadAll();
}

private String source;
private boolean save;
private String rssServletUrl;
private Map<String, Set<String>> urls = Maps.newHashMap();

public RSSFeedMonitor(String[] args) 
	throws MalformedURLException, IOException
{
	JobConfig config = new JobConfig(args);
	rssServletUrl = config.getFirstArg(URL_ARG);
	if (rssServletUrl==null)
		throw new RuntimeException("Must specify url to rss-servlet for rss feeds to monitor via command-line "+URL_ARG+"<url>");
	
	source = config.getSource();
	if (source==null)
		throw new RuntimeException("Must specify source name for HBase table via command-line "+JobConfig.SOURCE_ARG+"<source>");
	
	String value = config.getFirstArg(SAVE_ARG);
	if (value == null)
		save = true;
	else
		save = Boolean.parseBoolean(value);
	
}
	
public void getRssServlet() 
	throws MalformedURLException, IOException
{
	URLData data = new URLData(rssServletUrl);
	
    InputStreamReader jsonReader = new InputStreamReader(new ByteArrayInputStream(data.content.getBytes()));
    Gson g = new Gson();
    Type collectionType = new TypeToken<Map<String, Set<String>>>() {}.getType();
    urls = g.fromJson(jsonReader, collectionType);

    for (String feedUrl : urls.keySet()) {
    	ImportRSS.LOG.info("Monitor rss feed ("+feedUrl+")");
    }	
}

public void downloadAll() 
	throws IOException
{
	getRssServlet();
	
	
	long total=0;
	for (String feedUrl : urls.keySet()) {
		try {
			total += download(feedUrl);
		}
		catch (FeedException e) {
			e.printStackTrace();
		}
	}
	ImportRSS.LOG.info("Ingested a total of ("+total+") RSS entries/items");
}

public long download(String feedUrl) 
	throws IOException, IllegalArgumentException, FeedException
{
	Set<String> tagSet = urls.get(feedUrl);
	String[] tags = tagSet.toArray(new String[tagSet.size()]);
	String tagValue = Arrays.toString(tags);
	if (tagValue.length() > 0)
		tagValue = tagValue.substring(1,tagValue.length()-1);
	else
		tagValue = "${notset}";
	
	String[] args = { JobConfig.SOURCE_ARG+source,
			ImportRSS.SAVE_ARG+Boolean.toString(save),
			ImportRSS.URL_ARG+feedUrl,
			ImportRSS.TAGS_ARG+tagValue,
	};
	
	ImportRSS importRSS = new ImportRSS(args);
	return importRSS.ingest();
}
	
}
