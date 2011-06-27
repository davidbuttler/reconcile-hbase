package reconcile.nyt;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

import gov.llnl.text.util.MapUtil;
import gov.llnl.text.util.Timer;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.general.Constants;
import reconcile.hbase.table.DocSchema;


public class CreateDictionary {

static EntityList dict;

static int maxId = 0;
public static void main(String[] args)
{

  File outDir = new File(args[0]);
  String startKey = args[1];
  String endKey = args[2];
  try {
    PrintWriter out = new PrintWriter(new File(outDir, "matrix_" + startKey + ".tsv"));
    File entityFile = new File(outDir, "dict.txt");
    dict = new EntityList(entityFile);
    DocSchema doc = new DocSchema("NYT");
    Scan s = new Scan();
    s.addColumn(DocSchema.textCF.getBytes(), DocSchema.textRaw.getBytes());
    s.addColumn(DocSchema.annotationsCF.getBytes(), Constants.TOKEN.getBytes());
    s.addColumn(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes());
    s.addColumn(DocSchema.metaCF.getBytes(), "PublicationYear".getBytes());
    s.addColumn(DocSchema.metaCF.getBytes(), "PublicationMonth".getBytes());
    s.addColumn(DocSchema.metaCF.getBytes(), "PublicationDayOfMonth".getBytes());
    s.addColumn(DocSchema.metaCF.getBytes(), "OnlineSection".getBytes());
    s.setStartRow(startKey.getBytes());

    s.setFilter(new SingleColumnValueFilter(DocSchema.srcCF.getBytes(), DocSchema.srcName.getBytes(), CompareOp.EQUAL,
        "NYT".getBytes()));

    ResultScanner rs = doc.getScanner(s);
    Timer t = new Timer();
    System.out.println("iterate through db");
    for (Result row : rs) {
      t.increment();
      String key = new String(row.getRow(), Charsets.UTF_8);
      if (key.compareTo(endKey) > 0) {
        break;
      }

      String pubYear = DocSchema.getColumn(row, "PublicationYear");
      String pubMonth = DocSchema.getColumn(row, "PublicationMonth");
      String pubDay = DocSchema.getColumn(row, "PublicationDayOfMonth");
      String section = DocSchema.getColumn(row, "OnlineSection");
      String text = DocSchema.getRawText(row);
      AnnotationSet tokens = DocSchema.getAnnotationSet(row, Constants.TOKEN);
      if (pubYear == null || pubMonth == null || pubDay == null || section == null || text == null || tokens == null) {
        continue;
      }
      String sparseVector = getSparseVector(text, tokens);
      println(out, pubYear, pubMonth, pubDay, section, sparseVector);

    }
    System.out.println("write dictionary");

    t.end();

    out.flush();
    out.close();
    dict.close();
  }
  catch (IOException e) {
    e.printStackTrace();
  }
}

private static void println(PrintWriter out, String... data)
{
  for (int i = 0; i < data.length; i++) {
    out.print(data[i]);
    out.print("\t");
  }
  out.println();

}


@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
private static String getSparseVector(String text, AnnotationSet tokens)
{
  Map<Integer, Integer> count = Maps.newTreeMap();
  for (Annotation a : tokens) {
    String t = Annotation.getAnnotText(a, text);
    int id = getId(t);
    MapUtil.addCount(count, id);
  }

  StringBuffer b = new StringBuffer();
  for (Integer k : count.keySet()) {
    b.append(k).append(":").append(count.get(k)).append("\t");
  }
  return b.toString().trim();
}

private static int getId(String t)
{
  t = t.toLowerCase().trim();
  Integer id = dict.add(t);
  return id;
}
}
