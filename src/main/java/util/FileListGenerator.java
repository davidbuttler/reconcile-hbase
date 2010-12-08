package util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class FileListGenerator 
{
	public static final String LIST_FILE_ARG="-listFile=";
	public static final String INPUT_FILE_ARG="-inputFile=";

	public static Path process(String[] args) 
		throws IOException
	{
		HBaseConfiguration conf = new HBaseConfiguration();
		FileSystem fs = FileSystem.get(conf);

		String listFile = null;
		
		// Get list of possible input files from command-line arguments 
		ArrayList<String> inputFiles = new ArrayList<String>();
		for (String arg : args) {
			if (arg.startsWith(INPUT_FILE_ARG)) {
				String value = arg.substring(INPUT_FILE_ARG.length());
				if (! value.startsWith("$"))
				{	
					for (String val : value.split(",")) {
						inputFiles.add(val);
					}
				}
			}
			else if (arg.startsWith(LIST_FILE_ARG)) {
				listFile = arg.substring(LIST_FILE_ARG.length());
			}
		}
		
		if (listFile==null || listFile.length()==0) {
			throw new RuntimeException("HDFS list file name was not specified. Please provide via "+LIST_FILE_ARG+"<outputname>");
		}

		Path inputPath = new Path(listFile);
		  
		// Create list file from names of mbox files
		if (inputFiles.size() > 0) 
		{									
			OutputStream os = null;
			BufferedWriter writer = null;
			try {
				os = fs.create(inputPath, true);
				writer = new BufferedWriter(new OutputStreamWriter(os));
				for (String fileName :inputFiles) {
					writer.write(fileName+"\n");
				}
			} 
			catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to properly generate HDFS list file ("+listFile+")");
			}
			finally {
				IOUtils.closeQuietly(writer);
				IOUtils.closeQuietly(os);
			}
		}
		return inputPath;
	}
	
	public static void main(String[] args) throws IOException 
	{
		process (args);
	}
}
