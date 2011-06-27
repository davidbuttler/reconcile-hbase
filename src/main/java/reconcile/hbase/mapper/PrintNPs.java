package reconcile.hbase.mapper;

import static reconcile.hbase.table.DocSchema.textCF;
import static reconcile.hbase.table.DocSchema.textRaw;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.general.Constants;
import reconcile.hbase.mapreduce.ChainableAnnotationJob.AnnotateMapper;
import reconcile.hbase.table.DocSchema;

public class PrintNPs
    extends AnnotateMapper {



final DateFormat formatId = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS");

private int nFiles;

// boolean initialized = false;

/**
*
*/
public PrintNPs()
{

}

@Override
public void setup(Context context)
{
  try {
    super.setup(context);
  }
  catch (IOException e) {
    e.printStackTrace();
  }
  catch (InterruptedException e) {
    e.printStackTrace();
  }

}

@Override
public void cleanup(Context context1)
    throws IOException, InterruptedException
{
  super.cleanup(context1);
  System.out.println("processed " + nFiles + " files");
}

@Override
public void map(ImmutableBytesWritable mapKey, Result row, Context context)
    throws IOException, InterruptedException
{
  context.progress();
  System.out.println("time: " + formatId.format(new Date()));

  context.getCounter("printNPs", "seen row").increment(1);
    nFiles++;

    // Get the raw text
  String rawText = DocSchema.getColumn(row, textCF, textRaw);
    if (rawText == null || rawText.length() == 0) {
    context.getCounter("printNPs", "skipped -- no raw text").increment(1);
      System.out.println("skip, no raw text");
      return;
    }


    // ignore rows with data
  AnnotationSet parses = DocSchema.getAnnotationSet(row, Constants.PARSE);
  if (parses == null || parses.size() == 0) {
    context.getCounter("printNPs", "skipped -- not parsed").increment(1);
    return;
  }
  for (Annotation a : parses) {
    context.getCounter("type", a.getType()).increment(1);
  }

  AnnotationSet depAnnots = DocSchema.getAnnotationSet(row, Constants.DEP);
  if (depAnnots == null || depAnnots.size() == 0) {
    context.getCounter("printNPs", "skipped deps -- not parsed").increment(1);
    return;
  }
  for (Annotation a : depAnnots) {
    context.getCounter("dep type", a.getType()).increment(1);
  }



}





}
