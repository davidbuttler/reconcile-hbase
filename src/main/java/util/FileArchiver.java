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
package util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class FileArchiver {

	public static final Log LOG = LogFactory.getLog(FileArchiver.class);

	private Configuration conf;


	public static class MyPathFilter implements PathFilter
	{
		@Override
		public boolean accept(Path arg0) {
			return true;
		}
	}

@SuppressWarnings("rawtypes")
private class FileAccess
    implements Comparable
	{
		public FileSystem fs;
		public Path path;
		public Path basePath;

		public FileAccess(FileSystem fs, Path path) throws IOException
		{
			this.fs = fs; this.path = path; basePath = path;
			if (basePath.toString().contains("*")) {
				basePath = path.getParent();
			}
			basePath = fs.getFileStatus(basePath).getPath();
		}
		public FileAccess(FileAccess fa, Path path) {
			this.fs=fa.fs; this.basePath=fa.basePath; this.path=path;
		}
		@Override
		public int compareTo(Object o) {
			FileAccess other = (FileAccess) o;
			return this.path.compareTo(other.path);
		}
	}

	public FileArchiver()
		throws IOException
	{
		conf = new Configuration();
	}

	private void copyFile(InputStream is, OutputStream os)
		throws IOException
	{
		for (int c = is.read(); c != -1; c = is.read()) {
			os.write(c);
		}
	}

	private FileAccess gzipFile(FileAccess input)
		throws IOException
	{
		String outName = input.path.getName()+".gz";
		Path pathOut = new Path(input.path.getParent(), outName);
		LOG.info("....compressing file ("+input.path+") to file("+pathOut+")");
		FileAccess output = new FileAccess(input, pathOut);

		// Gzip the file before adding to archive
		InputStream is = null;  OutputStream os = null;
		try {
			is = input.fs.open(input.path);
			os = new GZIPOutputStream(output.fs.create(output.path, true));

			copyFile(is, os);
		}
		finally {
			IOUtils.closeQuietly(is);
			IOUtils.closeQuietly(os);
		}
		return output;
	}

	private void addFileToZip(ZipOutputStream os, FileAccess input, boolean gzipFiles)
		throws IOException
	{
		// Gzip input file first
		FileAccess output = input;
		if (gzipFiles) {
			output = gzipFile(input);
		}

		// Determine zip entry name
		LOG.debug("Base path("+output.basePath.toString()+")");
		LOG.debug("Output path("+output.path.toString()+")");
		//String entryName = output.path.getName();
		String entryName = output.path.toString().replaceFirst(output.basePath.toString(), "");
		if (entryName.startsWith("/")) {
			entryName = entryName.substring(1);
		}
		LOG.info("....creating archive entry ("+entryName+")");
		ZipEntry entry = new ZipEntry(entryName);

		FileStatus status = output.fs.getFileStatus(output.path);
		LOG.info("......setting size to ("+status.getLen()+")");
		try {
			entry.setSize(status.getLen());
		} catch (Exception e) {
			LOG.error(e.toString());
		}

		// Create new entry in archive
		os.putNextEntry(entry);

		// Copy compressed version of file into entry
		LOG.info("......adding data from file ("+output.path+")");
		InputStream is = null;
		try {
			is = output.fs.open(output.path);
			copyFile(is, os);
		}
		finally {
			IOUtils.closeQuietly(is);
		}
		LOG.info("......closing archive entry");
		// Close entry
		os.closeEntry();
	}

	/**
	 * Extract the given input zip contents into the specified output directory.
	 *
	 * @param inputZip  The input zip file [if prefix is ResourceFile.HDFS_PREFIX, path in HDFS]
	 * @param outputDir  The output path to extract zip contents to. [if prefix is ResourceFile.HDFS_PREFIX, path in HDFS]
	 * @throws IOException
	 */
	public void extract(String inputZip, String outputDir)
		throws IOException
	{
		FileSystem localFS = FileSystem.getLocal(conf);
		FileSystem hdfs = FileSystem.get(conf);

		FileSystem outFS = localFS;
		if (ResourceFile.isHDFSFile(outputDir)) {
			outFS = hdfs;
			outputDir = ResourceFile.hdfsFileName(outputDir);
			LOG.info("Will extract to HDFS directory");
		}

		Path outputPath = new Path(outputDir);
		outFS.mkdirs(outputPath);

		InputStream is = ResourceFile.getInputStream(inputZip);
		if (is==null)
			throw new IOException("Unable to open input stream for zip ("+inputZip+")");

		// Loop over zip entries and create output file for each
		OutputStream os = null;
		ZipInputStream zis = null;
		ZipEntry zipEntry = null;
		try {
			zis = new ZipInputStream(is);
			while ((zipEntry = zis.getNextEntry()) != null)
			{
				Path destPath = new Path(outputPath, zipEntry.getName());
				os = outFS.create(destPath, true);

				byte[] data = read(zipEntry.getName(), zis);
				os.write(data);
				os.flush();

				LOG.info("Extracted entry ("+zipEntry.getName()+") to ("+destPath.getName()+")");

				// Cleanup handles
				zis.closeEntry();
				IOUtils.closeQuietly(os);
			}
			zis.close();
		}
		catch (IOException e) {

		}
		finally {
			IOUtils.closeQuietly(zis);
			IOUtils.closeQuietly(os);
			IOUtils.closeQuietly(is);
		}
	}

	public static final int bufSize = 1024;
	private static byte[] read(String entry, ZipInputStream zis)
	{
		ArrayList<byte[]> dataArray = new ArrayList<byte[]>();
		byte[] current = null;

		int i=0;
		int n=0;
	    try {
			while (zis.available()==1) {
				if (n%bufSize == 0) {
					current = new byte[bufSize];
					dataArray.add(current);
					i=0;
				}
				current[i] = (byte) zis.read();
				++n; ++i;
			}
		}
	    catch (IOException e) {
	    	LOG.error("failure reading zip entry("+entry+")");
			e.printStackTrace();
			return null;
		}
	    --n;

	    // Copy multiple buffers into single large buffer
	    byte[] data = new byte[n];
	    i=0;
	    for (byte[] buffer : dataArray) {
	    	int copyLength = bufSize;
	    	if ( (i+copyLength) > n) {
	    		copyLength = n - i;
	    	}
	    	for (int j=0; j<copyLength; ++j) {
	    		data[i] = buffer[j];
	    		++i;
	    	}
	    }

		LOG.info("Read bytes("+n+") from entry ("+entry+")");
		LOG.debug("Read value("+Bytes.toString(data)+") from entry ("+entry+")");

		return data;
	}


	/**
	 * Create a zip archive containing non-recursive contents of the inputPaths as separate entries, and
	 * gzip each file before adding to archive.
	 *
	 * @param outputURI   The output zip file [if prefix is ResourceFile.HDFS_PREFIX, will be in HDFS]
	 * @param inputPaths  The input paths to be archived. [if prefix is ResourceFile.HDFS_PREFIX, input from HDFS]
	 * @throws IOException
	 */
	public void archive(String outputURI, Collection<String> inputPaths)
		throws IOException
	{
		archive(outputURI, inputPaths, true, false, false);
	}


	@SuppressWarnings("deprecation")
	private void getPaths(FileAccess input, Collection<FileAccess> paths, Collection<FileAccess> visited,
			boolean recursive)
		throws IOException
	{
		if (visited.contains(input))
			return;

		visited.add(input);

		try {
			LOG.debug("Get recursive paths called ("+input.path+")");

			if (input.fs.isDirectory(input.path))
			{
				if (recursive)
				{
					FileStatus[] files = input.fs.listStatus(input.path);
					for (FileStatus status : files) {
						getPaths(new FileAccess(input, status.getPath()), paths, visited, recursive);
					}
				}
			}
			else if (input.fs.isFile(input.path)) {
				LOG.debug("...adding to final paths("+input.path+")");
				paths.add(input);
			}
			else {
				// This should be a pattern since it is not a file or a directory
				FileStatus[] statuses = input.fs.globStatus(input.path);
				for (FileStatus status : statuses) {
					getPaths(new FileAccess(input, status.getPath()), paths, visited, recursive);
				}
			}
		}
		catch (IllegalArgumentException e) {
			e.printStackTrace();
			LOG.warn("Unable to process path ("+input.path+")");
		}
	}

	/**
	 * Create a zip archive containing the contents of the inputPaths as separate entries.
	 *
	 * @param outputURI   The output zip file [if prefix is ResourceFile.HDFS_PREFIX, will be in HDFS]
	 * @param inputPaths  The input paths to be archived. [if prefix is ResourceFile.HDFS_PREFIX, input from HDFS]
	 * @param gzipFiles   Boolean indicating whether to individually gzip each file before adding to archive
	 * @param recursive   Boolean indicating whether to recursively add files from input paths
	 * @param estimateTotalSizeOnly  Boolean indicating whether to just estimate the overall content size, or to truly compress
	 * 
	 * @return If estimateTotalSizeOnly is true, then total byte size of intended contents otherwise 0
	 * @throws IOException
	 */
	public double archive(String outputURI, Collection<String> inputPaths,
			boolean gzipFiles, boolean recursive, boolean estimateTotalSizeOnly)
		throws IOException
	{
		ZipOutputStream os=null;
		FileSystem localFS = FileSystem.getLocal(conf);
		FileSystem hdfs = FileSystem.get(conf);

		if (gzipFiles && estimateTotalSizeOnly) {
			throw new RuntimeException("Can not both estimate total size and gzip individual files");
		}
		
		try {
			// Get all recursive paths first
			TreeSet<FileAccess> finalPaths = new TreeSet<FileAccess>();
			TreeSet<FileAccess> visited = new TreeSet<FileAccess>();
			for (String pathName : inputPaths)
			{
				FileAccess fa = null;

				if (ResourceFile.isHDFSFile(pathName))
				{
					// Working with compressing input file on HDFS
					String hdfsPath = ResourceFile.hdfsFileName(pathName);
					fa = new FileAccess(hdfs, new Path(hdfsPath));
				}
				else {
					fa = new FileAccess(localFS, new Path(pathName));
				}

				getPaths(fa, finalPaths, visited, recursive);
			}

			if (estimateTotalSizeOnly) {
				LOG.info("Estimating uncompressed total file size");
				double sum = 0;
				for (FileAccess input : finalPaths) {
					double fileSize = input.fs.getFileStatus(input.path).getLen();
					sum += fileSize;
				}				
				return sum;
			}
			else {
				LOG.info("Creating archive file ("+outputURI+")");
				if (ResourceFile.isHDFSFile(outputURI)) {
					String hdfsFileName = ResourceFile.hdfsFileName(outputURI);
					os = new ZipOutputStream(hdfs.create(new Path(hdfsFileName)));
				}	
				else {
					os = new ZipOutputStream(ResourceFile.getOutputStream(outputURI));
				}
				// Add files to zip
				for (FileAccess input : finalPaths) {
					addFileToZip(os, input, gzipFiles);
				}
			}
		}
		finally {
			IOUtils.closeQuietly(os);
		}
		LOG.info("closing archive file");
		return 0;
	}

	public static final String EXTRACT_ARG = "-extract";
	public static final String OUTPUT_ARG = "-output=";
	public static final String INPUT_ARG = "-input=";
	public static final String GZIP_ARG = "-gzipFiles=";
	public static final String RECURSIVE_ARG = "-recursive=";

	public static void main(String[] args)
		throws IOException
	{
		boolean extract = false;
		boolean recursive = false;
		boolean gzipFiles = true;
		String outputFile = null;
		ArrayList<String> inputPaths = new ArrayList<String>();

		for (String arg : args)
		{
			if (arg.startsWith(OUTPUT_ARG)) {
				String value = arg.substring(OUTPUT_ARG.length());
				if (!value.startsWith("$")) {
					outputFile = value;
				}
			}
			else if (arg.startsWith(INPUT_ARG)) {
				String value = arg.substring(INPUT_ARG.length());
				if (!value.startsWith("$")) {
					for (String path : value.split(",")) {
						inputPaths.add(path);
					}
				}
			}
			else if (arg.startsWith(GZIP_ARG)) {
				String value = arg.substring(GZIP_ARG.length());
				if (!value.startsWith("$")) {
					gzipFiles = Boolean.parseBoolean(value);
				}
			}
			else if (arg.startsWith(RECURSIVE_ARG)) {
				String value = arg.substring(RECURSIVE_ARG.length());
				if (!value.startsWith("$")) {
					recursive = Boolean.parseBoolean(value);
					LOG.info("Recursive set to :"+recursive);
				}
			}
			else if (arg.startsWith(EXTRACT_ARG)) {
				extract = true;
			}
		}
		FileArchiver archiver = new FileArchiver();

		if (extract) {
			archiver.extract(inputPaths.get(0), outputFile);
		}
		else {
			archiver.archive(outputFile, inputPaths, gzipFiles, recursive, false);
		}
	}
}
