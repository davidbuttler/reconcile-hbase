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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import reconcile.hbase.mapreduce.JobConfig;

public class URLData
{
	public static final String TEXT_PLAIN="text/plain";
	public final String location;
	public final String contentType;
	public final String content;

	public URLData(String location) throws IOException, MalformedURLException
	{
		this.location = location;

		String s = null; 

		URL url = new URL(location);
		URLConnection urlConn = url.openConnection(); 
		urlConn.setDoInput(true); 
		urlConn.setUseCaches(false);
	
		contentType = urlConn.getContentType();
	
		BufferedReader reader = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
	
		StringBuffer buffer = new StringBuffer();
		while ((s = reader.readLine()) != null) { 
			buffer.append(s+"\n");
		}	 
		reader.close();
		
		content = buffer.toString();
	}
	
	public static final String PRINT_ARG="-print=";
	public static final String URL_ARG="-url=";
	public static final String CONTAINS_ARG="-contains=";
	
	public static void main(String[] args) 
		throws MalformedURLException, IOException
	{
		boolean print=true;
		
		JobConfig config = new JobConfig(args);
		String value = config.getFirstArg(PRINT_ARG);
		if (value!=null)
			print = Boolean.parseBoolean(value);
		
		String contains = config.getFirstArg(CONTAINS_ARG);
		
		String url = config.getFirstArg(URL_ARG);
		if (url==null)
			throw new RuntimeException("url was not provided. Must specify via command-line "+URL_ARG+"<url>");
		
		URLData data = new URLData(url);
		if (print) {
			System.out.println(data.content);
		}
		if (contains!=null) {
			if (! data.content.toLowerCase().contains(contains.toLowerCase())) {
				if (!print) {
					System.err.println("ERROR: expected content not found in:\n"+data.content);
					System.err.flush();
				}
				throw new RuntimeException("Retrieved content did not contain [case-insensitive] ("+contains+")");
			}
		}
	}
}