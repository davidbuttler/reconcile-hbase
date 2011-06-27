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

import reconcile.hbase.mapreduce.JobConfig;
import reconcile.hbase.table.DocSchema;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.syndication.feed.synd.*;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.SyndFeedOutput;
import com.sun.syndication.io.XmlReader;

public class ImportRSS 
{
	
public static final String URL_ARG ="-url=";
public static final String SAVE_ARG="-save=";
public static final String TAGS_ARG="-tags=";

public static final Log LOG = LogFactory.getLog(ImportRSS.class);


public static void main(String[] args) 
	throws IOException, IllegalArgumentException, FeedException
{
	if (args.length==0) {
		String[] myargs={ "-save=false", 
			//URL_ARG+"https://publicaffairs.llnl.gov/news/rssfeed.xml",
			//URL_ARG+"http://www.paperoftheweek.com/feed/atom/",
			URL_ARG+"http://www.googlelabs.com/rss",
			//URL_ARG+"http://www.cloudera.com/blog/feed/",
			//URL_ARG+"http://rss.cnn.com/rss/money_latest.rss",
			JobConfig.SOURCE_ARG+"ignored"
		};
		args = myargs;
	}
	 
	ImportRSS importer = new ImportRSS(args);
	importer.ingest();	
}

private String urlName;
private String tags;
private String source;
private boolean save;

public ImportRSS(String[] args) 
	throws IOException
{
	// Parse arguments
	JobConfig config = new JobConfig(args);
	source = config.getSource();
	if (source == null)
		throw new RuntimeException("Must specify source name for RSS/Atom feed via command-line "+JobConfig.SOURCE_ARG+"<source>");
	
	urlName = config.getFirstArg(URL_ARG);
	if (urlName == null)
		throw new RuntimeException("Must specify url for RSS/Atom feed via command-line "+URL_ARG+"<url>");
	
	tags = config.getFirstArg(TAGS_ARG);
	
	save = true;
	String value = config.getFirstArg(SAVE_ARG);
	if (value!=null) {
		save = Boolean.parseBoolean(value);
	}
}

public long ingest() 
	throws MalformedURLException, IOException, IllegalArgumentException, FeedException
{
	DocSchema docTable = null;
	if (save) {
		docTable = new DocSchema(source);
	}

	LOG.info("Get RSS feed ("+urlName+")");
	
	// Process feed XML as stream using ROME
	URL feedUrl = new URL(urlName);
	SyndFeedInput input = new SyndFeedInput();
    SyndFeed sf = input.build(new XmlReader(feedUrl));
	
    SyndFeedOutput output = new SyndFeedOutput();
    String content = output.outputString(sf);

    // Get the original XML for each item
	ArrayList<String> items = processAtomEntries(content);
	if (items.size()==0) {
		items = processRSSItems(content);
	}

	// Verify that XML Item count equals Feed Entry count
	if (items.size()!=sf.getEntries().size()) 
		throw new IOException("XML entries length does not match SyndFeed entries length");

	long numSaved = 0;
	for (int i=0; i<items.size(); ++i)
	{
		try {
			SyndEntry entry = (SyndEntry) sf.getEntries().get(i);
			String entryData = items.get(i);
			saveEntry(sf, entry, entryData, docTable);
			++numSaved;
		}
		catch (IOException e) {
			LOG.error("Unable to save entry ("+i+") as received exception. "+e.getMessage());
		}
	}
	
    // Flush table writes
	if (docTable!=null) {
		docTable.flushCommits();
		docTable.close();
	}
	return numSaved;
}

/**
 * Save the given SyndEntry into HBase as a row
 * @param feed
 * @param entry
 * @param entryData
 * @param docTable
 * @throws IOException
 */
private void saveEntry(SyndFeed feed, SyndEntry entry, String entryData, DocSchema docTable) 
	throws IOException 
{
	String id = DigestUtils.shaHex(entryData.getBytes());

	LOG.debug("Saving entry ("+id+") title("+entry.getTitle()+")");
	
	Put put = new Put(id.getBytes());

	DocSchema.add(put, DocSchema.srcCF, DocSchema.srcId, id);
	DocSchema.add(put, DocSchema.textCF, DocSchema.textOrig, entryData);

	// Save Description
	if (entry.getDescription()!=null) {
		String desc = entry.getDescription().getValue();
		DocSchema.add(put, DocSchema.metaCF, "description", desc);
	}

	meta(put, feed, entry);

	// Save separate entries for contents
	int numContent = 0;
	for (Object obj : entry.getContents()) {
		SyndContent cnt = (SyndContent) obj;
		if (saveEntryContent(cnt, "content", feed, entry, id, docTable))
			++numContent;
	}

	// Entry had no real content, store description as a content row
	if (numContent==0 && entry.getDescription()!=null) {
		saveEntryContent(entry.getDescription(), "desc", feed, entry, id, docTable);
	}
	
	if (docTable!=null)
		docTable.put(put);
	else
		echo(put);
}

private void add(Put put, String qualifier, Collection<String> vals)
{
	if (vals.size()==0) 
		return;
	String[] values = vals.toArray(new String[vals.size()]);
	String output = Arrays.toString(values);
	DocSchema.add(put, DocSchema.metaCF, qualifier, output.substring(1,output.length()-1));		
}

/**
 * Add feed and entry fields as meta data to put
 * 
 * @param put
 * @param feed
 * @param entry
 */
private void meta(Put put, SyndFeed feed, SyndEntry entry)
{
	DocSchema.add(put, DocSchema.srcCF, DocSchema.srcName, source);
	DocSchema.add(put, DocSchema.srcCF, "tags", tags);

	// Add Feed Meta data
	DocSchema.add(put, DocSchema.metaCF, "feed_url", feed.getUri());	
	DocSchema.add(put, DocSchema.metaCF, "feed_link", feed.getLink());	
	DocSchema.add(put, DocSchema.metaCF, "feed_author", feed.getAuthor());
	DocSchema.add(put, DocSchema.metaCF, "feed_copyright", feed.getCopyright());
	DocSchema.add(put, DocSchema.metaCF, "feed_description", feed.getDescription());
	DocSchema.add(put, DocSchema.metaCF, "feed_language", feed.getLanguage());
	DocSchema.add(put, DocSchema.metaCF, "feed_type", feed.getFeedType());
	DocSchema.add(put, DocSchema.metaCF, "feed_title", feed.getTitle());
	
	// Add Entry Meta data
	DocSchema.add(put, DocSchema.metaCF, "title", entry.getTitle());
	DocSchema.add(put, DocSchema.metaCF, "url", entry.getUri());
	DocSchema.add(put, DocSchema.metaCF, "updated_date", DocSchema.formatSolrDate(entry.getUpdatedDate()));
	DocSchema.add(put, DocSchema.metaCF, "published_date", DocSchema.formatSolrDate(entry.getPublishedDate()));
	
	// Save authors
	TreeSet<String> values = new TreeSet<String>();
	if (entry.getAuthor()!=null)
		values.add(entry.getAuthor());	
	if (entry.getAuthors()!=null)
		for (Object obj : entry.getAuthors()) {
			SyndPerson author = (SyndPerson) obj;
			values.add(author.getName());
		}
	add(put, "authors", values);		
	
	// Save links
	values.clear();
	if (entry.getLink()!=null)
		values.add(entry.getLink());
	if (entry.getLinks()!=null)		
		for (Object obj : entry.getLinks()) {
			SyndLink link = (SyndLink) obj;
			values.add(link.getHref());
		}
	add(put, "links", values);		

	// Save categories
	values.clear();
	if (entry.getCategories()!=null)
		for (Object obj : entry.getCategories()) {
			SyndCategory cat = (SyndCategory) obj;
			values.add(cat.getName());
		}
	add(put, "categories", values);		
	
	DocSchema.addIngestDate(put);	
}

/**
 * Save the given SyndContent as a separate row in HBase with a link back to SyndEntry row
 * @param content
 * @param tagType
 * @param feed
 * @param entry
 * @param entryKey
 * @param docTable
 * @return
 * @throws IOException
 */
private boolean saveEntryContent(SyndContent content, String tagType, 
		SyndFeed feed, SyndEntry entry, String entryKey, DocSchema docTable) 
throws IOException
{
	if (content.getValue() == null || content.getValue().length()==0) {
		LOG.error("entry row("+entryKey+") SyndContent value is empty.  Will not store as separate row.");
		return false;
	}
	
	String id = DigestUtils.shaHex(content.getValue().getBytes());
	Put put = new Put(id.getBytes());

	meta(put, feed, entry);
	
	DocSchema.add(put, DocSchema.srcCF, DocSchema.srcId, id);

	DocSchema.add(put, DocSchema.textCF, DocSchema.textType, content.getType());
	DocSchema.add(put, DocSchema.textCF, DocSchema.textOrig, content.getValue());

	DocSchema.add(put, DocSchema.metaCF, "entry_tag_type", tagType);
	DocSchema.add(put, DocSchema.metaCF, "entry_key", entryKey);
	
	if (docTable!=null)
		docTable.put(put);	
	else
		echo(put);
	
	return true;
}
	
private ArrayList<String> processAtomEntries(String content)
{
	ArrayList<String> entries = process(content, "<entry", "</entry>");
	LOG.info("There were ("+entries.size()+") atom entries found");
	return entries;
}

private ArrayList<String> processRSSItems(String content)
{
	ArrayList<String> items = process(content, "<item", "</item>");
	LOG.info("There were ("+items.size()+") rss items found");
	return items;
}

private ArrayList<String> process(String content, String BEGIN, String END)
{
	ArrayList<String> items = new ArrayList<String>();
	String caseInsensitive = content.toLowerCase();

	int ndx = -1;
	int endNdx = 0;
	while ( (ndx = caseInsensitive.indexOf(BEGIN, endNdx)) >= 0) {
		endNdx = caseInsensitive.indexOf(END, ndx+1);
		if (endNdx > ndx) 
		{
			// Found Entry
			endNdx += END.length();			
			String item = content.substring(ndx, endNdx);
			items.add(item);
		}	
	}
	return items;	
}

private void echo(Put put)
{
	System.out.println("Key("+Bytes.toString(put.getRow())+")================================================");
	for (byte[] family : put.getFamilyMap().keySet()) {
		String familyCF = Bytes.toString(family);
		for (KeyValue kv : put.getFamilyMap().get(family)) {
			System.out.println("col("+familyCF+":"+Bytes.toString(kv.getQualifier())+") value("+Bytes.toString(kv.getValue())+")");
		}
	}
	
}

}
