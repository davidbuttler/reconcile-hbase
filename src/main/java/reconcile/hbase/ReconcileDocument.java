package reconcile.hbase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import reconcile.data.Annotation;
import reconcile.data.AnnotationSet;
import reconcile.general.Utils;
import reconcile.hbase.table.DocSchema;

public class ReconcileDocument
    extends reconcile.data.Document {

public static final Pattern pTab = Pattern.compile("\t");


// private Map<String, AnnotationSet> mAnnotationSets;



private String mId;

private ReconcileDocument() {
  mMetaData = Maps.newHashMap();
  mAnnotationSets = Maps.newHashMap();

}
public ReconcileDocument(Result row) {
  this();
  try {
    mText = DocSchema.getRawText(row);
    if (mText == null) {
      mText = "";
    }
    getMetaData(row);
    preloadAnnotationSets(row);
    mId = new String(row.getRow(), HConstants.UTF8_ENCODING);
  }
  catch (UnsupportedEncodingException e) {
    e.printStackTrace();
  }
}

public ReconcileDocument(String string) {
  this();
  mText = string;
  mId = String.valueOf(string.hashCode());
}

@Override
public int hashCode()
{
  return mText.hashCode();
}

@Override
public boolean equals(Object obj)
{
  if (!(obj instanceof ReconcileDocument)) return false;
  ReconcileDocument d = (ReconcileDocument) obj;
  return Objects.equal(mId, d.mId);
}

@Override
public String getText()
{
  return mText;
}

@Override
public String getDocumentId()
{
    return mId;
}

@Override
public Set<String> getMetaDataKeys()
{
  return mMetaData.keySet();
}

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "WMI_WRONG_MAP_ITERATOR")
private void getMetaData(Result row)
{
  try {
    Map<byte[], byte[]> map = row.getFamilyMap(DocSchema.metaCF.getBytes());
    for (byte[] key : map.keySet()) {
      String keyStr = new String(key, HConstants.UTF8_ENCODING).trim();
      String valStr = new String(map.get(key), HConstants.UTF8_ENCODING);
      mMetaData.put(keyStr, valStr);
    }
  }
  catch (UnsupportedEncodingException e) {
    throw new RuntimeException(e);
  }
}

public static Set<String> parse(String valStr)
{
  Set<String> result = Sets.newHashSet(Iterables.transform(Sets.newHashSet(pTab.split(valStr)),
      new Function<String, String>() {

    @Override
    public String apply(String arg)
    {
      return arg.trim();
    }
  }));
  return result;
}


@Override
public AnnotationSet getAnnotationSet(String annotationName)
{
  AnnotationSet as = null;
  if (mAnnotationSets.containsKey(annotationName)) {
    as = mAnnotationSets.get(annotationName);
  }
  if (as == null) {
    as = new AnnotationSet(annotationName);
  }
  return as;
}

private void preloadAnnotationSets(Result row)
{
  Map<byte[], byte[]> m = row.getFamilyMap(DocSchema.annotationsCF.getBytes());
  for (byte[] annotationName : m.keySet()) {
    try {
      String name = new String(annotationName, HConstants.UTF8_ENCODING);
      AnnotationSet as = DocSchema.getAnnotationSet(row, name);
      mAnnotationSets.put(name, as);
    }
    catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

}

@Override
public String getAnnotText(Annotation a)
{
  String result = getAnnotString(a);
  if (result == null) return null;
  result = result.trim().replaceAll("(\\s|\\n)+", " ");
  result = result.replaceAll("\\A[\\s\\\"'`\\[\\(\\-]+", "").replaceAll("\\W+\\z", "");
  return result;
}



@Override
public void deleteAnnotation(String name)
    throws IOException
{
  String canName = getCannonicalAnnotationSetName(name);
  mAnnotationSets.remove(canName);
}

@Override
public void deleteClusterFile()
    throws IOException
{
  throw new RuntimeException("not implemented");
}

@Override
public void deleteFeatureFile()
    throws IOException
{
  throw new RuntimeException("not implemented");
}

@Override
public void deletePredictionFile()
    throws IOException
{
  throw new RuntimeException("not implemented");
}

/**
 * @param responseNps
 * @return
 */

@Override
public boolean existsAnnotationSetFile(String asName)
{
  AnnotationSet as = getAnnotationSet(asName);
  return as != null;
}

@Override
public boolean existsClusterFile()
{
  throw new RuntimeException("not implemented");
}

@Override
public boolean existsFeatureFile()
{
  throw new RuntimeException("not implemented");
}

@Override
public boolean existsFile(String name)
{
  throw new RuntimeException("not implemented");
}

@Override
public boolean existsPredictionFile()
{
  throw new RuntimeException("not implemented");
}

/**
 * @return
 */
@Override
public String getAbsolutePath()
{
  throw new RuntimeException("not implemented");

}

// /////////////////////////////////////////////////////////////////////////////////////////////

@Override
public File getAnnotationSetFile(String annotationSetName)
{
  throw new RuntimeException("not implemented");
}

@Override
public Set<String> getAnnotationSetNames()
{
  return mAnnotationSets.keySet();
}

@Override
public String getAnnotString(Annotation a)
{
  return getAnnotString(a.getStartOffset(), a.getEndOffset());
}

@Override
public String getAnnotString(int start, int end)
{
  String result = null;

  try {
    result = getText().substring(start, end);
  }
  catch (StringIndexOutOfBoundsException siobe) {
    int st = mText.length() - 20;
    for (int i = st; i < mText.length(); i++) {
      System.err.println(i + ":" + mText.charAt(i));

    }
    throw new RuntimeException(siobe);
  }
  return result;
}

@Override
public String getAnnotText(int start, int end)
{
  String result = getAnnotString(start, end);
  if (result == null) return null;
  result = result.trim().replaceAll("(\\s|\\n)+", " ");
  result = result.replaceAll("\\A[\\s\\\"'`\\[\\(\\-]+", "").replaceAll("\\W+\\z", "");
  return result;
}

/**
 * Given an annotation set name, check to see if there is a cannonicalization for it, and if so, return that
 * canonicalization
 *
 * @param annotationSetName
 * @return
 */
@Override
public String getCannonicalAnnotationSetName(String annotationSetName)
{
  String annSetName = Utils.getConfig().getAnnotationSetName(annotationSetName);
  String name = null;
  if (annSetName != null) {
    name = annSetName;
  }
  else {
    name = annotationSetName;
  }
  return name;
}


/**
 * Get the cluster file associated with this document
 *
 * @return
 */
@Override
public File getClusterFile()
{
  throw new RuntimeException("not implemented");
}
@Override
public File getFeatureFile()
{
  throw new RuntimeException("not implemented");
}


@Override
public File getPredictionFile()
{
  throw new RuntimeException("not implemented");
}

/**
 * @return
 */
@Override
public File getRawFile()
{
  throw new RuntimeException("not implemented");
}


/**
 *
 * @return the directory that encapsulates all of the information for this particular document
 */
@Override
public File getRootDir()
{
  throw new RuntimeException("not implemented");
}




@Override
public InputStream readAnnotationDirFile(String filename)
{
  throw new RuntimeException("not implemented");
}


@Override
public InputStream readClusterDirFile(String fileName)
{
  throw new RuntimeException("not implemented");
}


@Override
public InputStream readClusterFile()
{
  throw new RuntimeException("not implemented");
}

@Override
public InputStream readFeatureDirFile(String filename)
{
  throw new RuntimeException("not implemented");
}

@Override
public InputStream readFeatureFile()
{
  throw new RuntimeException("not implemented");
}

@Override
public InputStream readFile(String name)
{
  throw new RuntimeException("not implemented");
}


@Override
public InputStream readPredictionDirFile(String fileName)
{
  throw new RuntimeException("not implemented");
}


@Override
public InputStream readPredictionFile()
{
  throw new RuntimeException("not implemented");
}


@Override
public void setDocumentId(String id)
{
  throw new RuntimeException("not implemented");
}

/**
 * @param inputTextFile
 * @throws IOException
 */
@Override
public void setRawText(File inputTextFile)
    throws IOException
{
  throw new RuntimeException("not implemented");
}

/**
 * @param articleNoTags
 * @throws IOException
 */
@Override
public void setRawText(String text)
{
  throw new RuntimeException("not implemented");
}

@Override
public OutputStream writeAnnotationDirFile(String filename)
{
  throw new RuntimeException("not implemented");
}

@Override
public void writeAnnotationDirFile(String filename, String content)
{
  throw new RuntimeException("not implemented");
}

@Override
public void writeAnnotationSet(AnnotationSet anSet)
{
  addAnnotationSet(anSet, anSet.getName(), true);
}

@Override
public OutputStream writeClusterDirFile(String fileName)
{
  throw new RuntimeException("not implemented");
}

@Override
public void writeClusterDirFile(String fileName, String content)
{
  throw new RuntimeException("not implemented");
}

@Override
public OutputStream writeClusterFile()
{
  throw new RuntimeException("not implemented");
}

@Override
public void writeClusterFile(String content)
{
  throw new RuntimeException("not implemented");
}

@Override
public OutputStream writeFeatureDirFile(String filename)
{
  throw new RuntimeException("not implemented");
}

@Override
public void writeFeatureDirFile(String filename, String content)
{
  throw new RuntimeException("not implemented");
}

@Override
public OutputStream writeFeatureFile()
{
  throw new RuntimeException("not implemented");
}

@Override
public void writeFeatureFile(String content)
{
  throw new RuntimeException("not implemented");
}

@Override
public void writeFile(File file, String content)
{
  throw new RuntimeException("not implemented");
}

@Override
public OutputStream writeFile(String name)
{
  throw new RuntimeException("not implemented");
}

@Override
public void writeFile(String name, String content)
{
  throw new RuntimeException("not implemented");
}

@Override
public OutputStream writePredictionDirFile(String fileName)
{
  throw new RuntimeException("not implemented");
}


@Override
public void writePredictionDirFile(String fileName, String content)
{
  throw new RuntimeException("not implemented");
}

@Override
public OutputStream writePredictionFile()
{
  throw new RuntimeException("not implemented");
}

@Override
public void writePredictionFile(String content)
{
  throw new RuntimeException("not implemented");
}

/**
 * @return
 */
@Override
public int length()
{
  return getText().length();
}

/**
 * Add field meta data to the document.
 *
 * @param key
 * @param val
 * @throws IOException
 */
@Override
public synchronized void putMetaData(String key, String val)
    throws IOException
{
  mMetaData.put(key, val);
}

@Override
public synchronized void removeMetaData(String key)
    throws IOException
{
  mMetaData.remove(key);
}

/**
 * Add field meta data to the document.
 *
 * @param data
 *          a map containing several values that should be added
 * @throws IOException
 */
@Override
public synchronized void addMetaData(Map<String, String> data)
    throws IOException
{
  mMetaData.putAll(data);
}

/**
 * add an annotation set to the document with the given name. IGNORES the <code>write</code> parameter, so this
 * annotation is just stored in memory
 *
 * @param set
 * @param annotationSetName
 *          This name will be checked against the config file to see if it needs to be mapped to a different name
 * @param write
 *          A flag determining whether the annotation set should be written to disk or just cached in memory
 * @throws IOException
 */
@Override
public void addAnnotationSet(AnnotationSet set, String annotationSetName, boolean write)
{
  String annSetName = getCannonicalAnnotationSetName(annotationSetName);
  mAnnotationSets.put(annSetName, set);
}

public void addAnnotationSet(AnnotationSet set, String annotationSetName)
{
  addAnnotationSet(set, annotationSetName, false);
}

}
