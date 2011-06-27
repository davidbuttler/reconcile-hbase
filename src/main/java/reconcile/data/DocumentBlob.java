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
 *
 * Created on Apr 29, 2009
 *
 * $Id: DocumentBlob.java,v 1.3 2009/05/11 21:38:02 buttler1 Exp $
 *
 */
package reconcile.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

import gov.llnl.text.util.FileUtils;
import gov.llnl.text.util.MapUtil;

import util.StopWord;

import reconcile.SystemConfig;
import reconcile.general.Constants;
import reconcile.general.Utils;
import reconcile.util.MemoryEntityList;
import reconcile.util.MemoryFileSystem;

/**
 * An encapsulation of the text and associated meta data and annotations associated with a document
 *
 * @author David Buttler
 *
 */
public class DocumentBlob
    extends Document {

static private boolean DEBUG = false;

private MemoryFileSystem inMemFileSystem = new MemoryFileSystem();
private BiMap<Integer, String> mDictionary;
private Map<Integer, Integer> mTermFreq;
private static final Pattern pSpace = Pattern.compile("\\s+");

/**
 * Given the text of a document, create a new DocumentBlob
 *
 * @param parentFile
 * @throws IOException
 */
public DocumentBlob(String id, String rawText) {
  super();
  mDocId = id;
  setRawText(rawText);
}

/**
 *
 */
private DocumentBlob() {
}

@Override
protected void init() {
  mAnnotationSets = Maps.newHashMap();
  mMetaData = Maps.newHashMap();
}
/**
 * Returns an annotation set. If it is a keyed annotation set from the config file (e.g. one of the X_ANNOTATION
 * constants in this class), the name may be switched by different configuration options. Else, the given name is used.
 * AnnotationSets are cached in memory so that there is no additional IO for repeated gets
 *
 * @param annotationSetName
 * @return
 * @throws IOException
 */
@Override
public AnnotationSet getAnnotationSet(String annotationSetName)
{
  String annSetName = getCannonicalAnnotationSetName(annotationSetName);
  AnnotationSet set = mAnnotationSets.get(annSetName);
  if (set == null) {
    String name = annotationSetName;
    if (!annSetName.equals(annotationSetName)) {
      name = annotationSetName + " (canonical: " + annSetName + ")";
    }
    throw new RuntimeException("Annotation file does not exist: " + name);
  }
  return set;
}

/**
 * write an annotation set to the document directory using the annotation set name. This name will be checked against
 * the config file to see if it needs to be mapped to a different name
 *
 * @param set
 * @throws IOException
 */
public void addAnnotationSet(AnnotationSet set)
{
  addAnnotationSet(set, set.getName(), true);
}

/**
 * write an annotation set to the document directory using the annotation set name. This name will be checked against
 * the config file to see if it needs to be mapped to a different name
 *
 * @param set
 * @throws IOException
 */
@Override
public void writeAnnotationSet(AnnotationSet set)
{
  addAnnotationSet(set, set.getName(), true);
}

/**
 * write an annotation set to the document directory using the annotation set name. This name will be checked against
 * the config file to see if it needs to be mapped to a different name
 *
 * @param set
 * @throws IOException
 */
public void addAnnotationSet(AnnotationSet set, String setName)
{
  addAnnotationSet(set, setName, true);
}

/**
 * @param set
 *          annotation set
 * @param write
 * @throws IOException
 */
@Override
public void addAnnotationSet(AnnotationSet set, boolean write)
{
  addAnnotationSet(set, set.getName(), write);
}

/**
 * add an annotation set to the document directory with the given name. Depending on the <code>write</code> parameter,
 * the annotation set may be written to disk, or just stored in memory
 *
 * @param set
 * @param annotationSetName
 *          This name will be checked against the config file to see if it needs to be mapped to a different name
 * @param write
 *          A flag determining whether the annotation set should be written to disk or just cached in memory
 */
@Override
public void addAnnotationSet(AnnotationSet set, String annotationSetName, boolean write)
{
  String annSetName = getCannonicalAnnotationSetName(annotationSetName);
  mAnnotationSets.put(annSetName, set);
  if (write) {
    File f = new File(getAnnotationDir(), annSetName);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    PrintWriter out = new PrintWriter(output);
    AnWriter.write(set, out);
    out.flush();
    inMemFileSystem.put(f.getPath(), output);
  }
}

public Map<Integer, String> getLocalDictionary()
{

  if (mDictionary == null) {
    createDictionary();
  }
  return mDictionary;
}

public Map<Integer, Integer> getLocalTermFrequency()
{

  if (mTermFreq == null) {
    createDictionary();
  }
  return mTermFreq;
}

private void createDictionary()
{
  MemoryEntityList dict = new MemoryEntityList();
  mTermFreq = new TreeMap<Integer, Integer>();
  StopWord stop = null;
  try {
    String stopFileName = Corpus.getConfig().getString(Corpus.STOP_FILE);
    if (stopFileName != null) {
      stop = new StopWord(new File(stopFileName));
    }
  }
  catch (Exception e) {
    e.printStackTrace();
  }
  AnnotationSet tokens = getAnnotationSet(Constants.TOKEN);
  AnnotationSet nes = getAnnotationSet(Constants.NE);
  for (Annotation t : nes) {
    String token = getAnnotString(t);
    token = pSpace.matcher(token).replaceAll(" ").trim();
    int wordId = dict.add(token);
    MapUtil.addCount(mTermFreq, wordId);
  }
  for (Annotation t : tokens) {
    if (nes.containsSpan(t)) {
      continue;
    }
    String token = getAnnotString(t);
    if (stop == null || !stop.isStopWord(token)) {
      String stem = token;
      if (stop != null) {
        stem = stop.stem(token);
      }
      stem = pSpace.matcher(stem).replaceAll(" ").trim();
      int wordId = dict.add(stem);
      MapUtil.addCount(mTermFreq, wordId);
    }
  }
  mDictionary = HashBiMap.create(dict);

}

/**
 * Add field meta data to the document. Note, that this is currently very inefficient and writes out all meta data on
 * each change
 *
 * @param key
 * @param val
 * @throws IOException
 */
@Override
public synchronized void putMetaData(String key, String val)
{
  mMetaData.put(key, val);
}

/**
 * Add field meta data to the document. Note, that this is currently very inefficient and writes out all meta data on
 * each change
 *
 * @param data
 *          a map containing several values that should be added
 * @throws IOException
 */
@Override
public synchronized void addMetaData(Map<String, String> data)
{
  mMetaData.putAll(data);
}

@Override
public String getMetaData(String key)
{
  return mMetaData.get(key);
}

/**
 * @param articleNoTags
 * @throws IOException
 */
@Override
public void setRawText(String text)
{
  mText = text;
}

/**
 * @return
 */
@Override
public File getRawFile()
{
  File f = new File(".", RAW_TXT);
  return f;
}

/**
 * @return
 */
@Override
public String getText()
{

  if (mText == null) throw new RuntimeException("text is null.  This will cause all sorts of problems: " + mDocId);

  return mText;
}

/**
 * @param inputTextFile
 * @throws IOException
 */
@Override
public void setRawText(File inputTextFile)
{
  throw new RuntimeException("File operations not supported in Blob");
}

/**
 *
 * @return the directory that encapsulates all of the information for this particular document
 */
@Override
public File getRootDir()
{
  return new File(".");
}

@Override
public String toString()
{
  return mDocId;
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
 * @return
 */
@Override
public String getDocumentId()
{
  return mDocId;
}

@Override
public void setDocumentId(String id)
{
  mDocId = id;
}

/**
 * @param key
 * @param bs
 */
public void addFile(String key, byte[] bs)
{
  if (RAW_TXT.equals(key)) {
    if (getText() == null || getText().length() == 0) {
      setRawText(new String(bs));
    }
  }
  else if (METADATA_FILE.equals(key)) {
    try {
      readMetaData(new ByteArrayInputStream(bs), mMetaData);
    }
    catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  else {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      OutputStreamWriter osw = new OutputStreamWriter(out);
      ByteArrayInputStream in = new ByteArrayInputStream(bs);
      InputStreamReader isr = new InputStreamReader(in);
      FileUtils.write(osw, isr);
      osw.flush();
      inMemFileSystem.put(key, out);
    }
    catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}

/**
 * delete the cluster file associated with this document
 */
@Override
public void deleteClusterFile()
{
  File f = getClusterFile();
  inMemFileSystem.delete(f);
}

/**
 * Get the cluster file associated with this document
 *
 * @return
 */
@Override
public File getClusterFile()
{
  File dir = getClusterDir();
  if (DEBUG) {
    System.out.println("Document.getClusterFile: fn=" + new File(dir, Constants.CLUSTER_FILE_NAME));
  }

  File mClusterFile = new File(dir, Constants.CLUSTER_FILE_NAME);

  return mClusterFile;
}

/**
 * get the cluster directory associated with this document
 */
@Override
public File getClusterDir()
{
  SystemConfig cfg = Utils.getConfig();
  String clName = cfg.getClusterer();
  File predDir = getPredictionDir();
  File mClusterSubDir = new File(predDir, clName);

  return mClusterSubDir;
}

@Override
public File getFeatureFile()
{
  File dir = getFeatureDir();
  SystemConfig cfg = Utils.getConfig();
  String featureFormat = cfg.getString(FEATURE_FORMAT, "arff");

  File mFeatureFile = new File(dir, Constants.FEAT_FILE_NAME + "." + featureFormat);
  return mFeatureFile;
}

@Override
public void deleteFeatureFile()
{
  File f = getFeatureFile();
  inMemFileSystem.delete(f);
}

@Override
public File getPredictionDir()
{
  SystemConfig cfg = Utils.getConfig();
  String classifierName = cfg.getClassifier();
  String modelName = cfg.getModelName();
  File featDir = getFeatureDir();
  File mPredictionDir = new File(featDir, Constants.PRED_DIR_NAME + "." + classifierName + "." + modelName);
  return mPredictionDir;
}

@Override
public File getPredictionFile()
{
  File mPredictionFile = new File(getPredictionDir(), Constants.PRED_FILE_NAME);
  return mPredictionFile;
}

@Override
public void deletePredictionFile()
{
  File f = getPredictionFile();
  inMemFileSystem.delete(f);
}

@Override
public File getAnnotationDir()
{
  File mAnnotationDir = new File(".", Constants.ANNOT_DIR_NAME);
  return mAnnotationDir;
}

/**
 * @return
 */
@Override
public File getFeatureDir()
{
  SystemConfig cfg = Utils.getConfig();
  String featSetName = cfg.getFeatSetName();
  File mFeatureDir = new File(".", Constants.FEAT_DIR_NAME + "." + featSetName);
  return mFeatureDir;
}

/* (non-Javadoc)
 * @see reconcile.Document#clean()
 */
@Override
public int clean()
{
  int count = 0;
  deleteClusterFile();
  count++;
  deleteFeatureFile();
  count++;
  deletePredictionFile();
  count++;

  return count;
}

/* (non-Javadoc)
 * @see reconcile.Document#readAnnotationDirFile(java.lang.String)
 */
@Override
public InputStream readAnnotationDirFile(String fileName)
{
  File dir = getAnnotationDir();
  File f = new File(dir, fileName);
  return readFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#readClusterDirFile(java.lang.String)
 */
@Override
public InputStream readClusterDirFile(String fileName)
{
  File dir = getClusterDir();
  File f = new File(dir, fileName);
  return readFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#readClusterFile()
 */
@Override
public InputStream readClusterFile()
{
  File f = getClusterFile();
  return readFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#readFeatureDirFile(java.lang.String)
 */
@Override
public InputStream readFeatureDirFile(String fileName)
{
  File dir = getFeatureDir();
  File f = new File(dir, fileName);
  return readFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#readFeatureFile()
 */
@Override
public InputStream readFeatureFile()
{
  File f = getFeatureFile();
  return readFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#readFile(java.lang.String)
 */
@Override
public InputStream readFile(String name)
{
  byte[] file = inMemFileSystem.get(name);
  if (file == null) return null;
  return new ByteArrayInputStream(file);
}

/* (non-Javadoc)
 * @see reconcile.Document#readPredictionDirFile(java.lang.String)
 */
@Override
public InputStream readPredictionDirFile(String fileName)
{
  File dir = getPredictionDir();
  File f = new File(dir, fileName);
  return readFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#readPredictionFile()
 */
@Override
public InputStream readPredictionFile()
{
  File f = getPredictionFile();
  return readFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#writeAnnotationDirFile(java.lang.String, java.io.InputStream)
 */
@Override
public OutputStream writeAnnotationDirFile(String fileName)
{
  File dir = getAnnotationDir();
  File f = new File(dir, fileName);
  return writeFile(f.getPath());

}
/* (non-Javadoc)
 * @see reconcile.Document#writeAnnotationDirFile(java.lang.String, java.io.InputStream)
 */
@Override
public void writeAnnotationDirFile(String fileName, String content)
{
  File dir = getAnnotationDir();
  File f = new File(dir, fileName);
  writeFile(f.getPath(), content);

}

/* (non-Javadoc)
 * @see reconcile.Document#writeClusterDirFile(java.lang.String, java.io.InputStream)
 */
@Override
public OutputStream writeClusterDirFile(String fileName)
{
  File dir = getClusterDir();
  File f = new File(dir, fileName);
  return writeFile(f.getPath());

}
/* (non-Javadoc)
 * @see reconcile.Document#writeClusterDirFile(java.lang.String, java.io.InputStream)
 */
@Override
public void writeClusterDirFile(String fileName, String content)
{
  File dir = getClusterDir();
  File f = new File(dir, fileName);
  writeFile(f.getPath(), content);

}

/* (non-Javadoc)
 * @see reconcile.Document#writeClusterFile(java.io.InputStream)
 */
@Override
public OutputStream writeClusterFile()
{
  File f = getClusterFile();
  return writeFile(f.getPath());
}
/* (non-Javadoc)
 * @see reconcile.Document#writeClusterFile(java.io.InputStream)
 */
@Override
public void writeClusterFile(String content)
{
  File f = getClusterFile();
  writeFile(f.getPath(), content);
}

/* (non-Javadoc)
 * @see reconcile.Document#writeFeatureDirFile(java.lang.String, java.io.InputStream)
 */
@Override
public OutputStream writeFeatureDirFile(String fileName)
{
  File dir = getFeatureDir();
  File f = new File(dir, fileName);
  return writeFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#writeFeatureDirFile(java.lang.String, java.io.InputStream)
 */
@Override
public void writeFeatureDirFile(String fileName, String content)
{
  File dir = getFeatureDir();
  File f = new File(dir, fileName);
  writeFile(f.getPath(), content);
}


/* (non-Javadoc)
 * @see reconcile.Document#writeFeatureFile(java.io.InputStream)
 */
@Override
public OutputStream writeFeatureFile()
{
  File f = getFeatureFile();
  return writeFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#writeFeatureFile(java.io.InputStream)
 */
@Override
public void writeFeatureFile(String content)
{
  File f = getFeatureFile();
  writeFile(f.getPath(), content);
}


/* (non-Javadoc)
 * @see reconcile.Document#writePredictionDirFile(java.lang.String, java.io.InputStream)
 */
@Override
public OutputStream writePredictionDirFile(String fileName)
{
  File dir = getPredictionDir();
  File f = new File(dir, fileName);
  return writeFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#writePredictionDirFile(java.lang.String, java.io.InputStream)
 */
@Override
public void writePredictionDirFile(String fileName, String content)
{
  File dir = getPredictionDir();
  File f = new File(dir, fileName);
  writeFile(f.getPath(), content);
}

/* (non-Javadoc)
 * @see reconcile.Document#writeFile(java.lang.String)
 */
@Override
public OutputStream writeFile(String name)
{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    inMemFileSystem.put(name, out);
    return out;
}
/* (non-Javadoc)
 * @see reconcile.Document#writeFile(java.lang.String, java.io.InputStream)
 */
@Override
public void writeFile(String name, String content)
{
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      FileUtils.write(out, content);
      inMemFileSystem.put(name, out);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
}

public void flush() {

}

/* (non-Javadoc)
 * @see reconcile.Document#writePredictionFile(java.io.ByteArrayInputStream)
 */
@Override
public OutputStream writePredictionFile()
{
  File f = getPredictionFile();
  return writeFile(f.getPath());
}

/* (non-Javadoc)
 * @see reconcile.Document#writePredictionFile(java.io.ByteArrayInputStream)
 */
@Override
public void writePredictionFile(String content)
{
  File f = getPredictionFile();
  writeFile(f.getPath(), content);
}

/* (non-Javadoc)
 * @see reconcile.Document#existsAnnotationSetFile(java.lang.String)
 */
@Override
public boolean existsAnnotationSetFile(String asName)
{
  File dir = getAnnotationDir();
  File f = new File(dir, asName);
  byte[] file = inMemFileSystem.get(f);
  return file != null;
}

/* (non-Javadoc)
 * @see reconcile.Document#existsClusterFile()
 */
@Override
public boolean existsClusterFile()
{
  File f = getClusterFile();
  byte[] file = inMemFileSystem.get(f);
  return file != null;
}

/* (non-Javadoc)
 * @see reconcile.Document#existsFeatureFile()
 */
@Override
public boolean existsFeatureFile()
{
  File f = getFeatureFile();
  byte[] file = inMemFileSystem.get(f);
  return file != null;
}

/* (non-Javadoc)
 * @see reconcile.Document#existsFile(java.lang.String)
 */
@Override
public boolean existsFile(String name)
{
  byte[] file = inMemFileSystem.get(name);
  return file != null;
}

/* (non-Javadoc)
 * @see reconcile.Document#existsPredictionFile()
 */
@Override
public boolean existsPredictionFile()
{
  File f = getPredictionFile();
  byte[] file = inMemFileSystem.get(f);
  return file != null;
}

/* (non-Javadoc)
 * @see reconcile.Document#deleteAnnotation(java.lang.String)
 */
@Override
public void deleteAnnotation(String name)
{
  File f = getAnnotationDir();
  String canName = getCannonicalAnnotationSetName(name);
  File df = new File(f, canName);
  inMemFileSystem.delete(df);

}

/* (non-Javadoc)
 * @see reconcile.Document#listAnnotationTypes()
 */
@Override
public List<String> listAnnotationTypes()
{
  File f = getAnnotationDir();
  String prefix = f.getPath();
  List<String> result = new ArrayList<String>();
  for (String name : inMemFileSystem.list()) {
    if (name.startsWith(prefix)) {
      result.add(name.substring(prefix.length() + 1));
    }
  }
  return result;
}

public byte[] serialize()
    throws IOException
{
  ByteArrayOutputStream output = new ByteArrayOutputStream();
  ObjectOutputStream oos = new ObjectOutputStream(output);
  oos.writeObject(mDocId);
  oos.writeObject(mText);
  oos.writeObject(mMetaData);
  oos.writeObject(mAnnotationSets);
  oos.writeObject(inMemFileSystem);
  oos.flush();
  return output.toByteArray();
}

@SuppressWarnings("unchecked")
public static DocumentBlob deserialize(InputStream in)
    throws IOException
{
  ObjectInputStream ois = new ObjectInputStream(in);
  DocumentBlob doc = new DocumentBlob();
  try {
    doc.mDocId = (String) ois.readObject();
    doc.mText = (String) ois.readObject();
    doc.mMetaData = (Map<String, String>) ois.readObject();
    doc.mAnnotationSets = (Map<String, AnnotationSet>) ois.readObject();
    doc.inMemFileSystem = (MemoryFileSystem) ois.readObject();

    return doc;
  }
  catch (ClassNotFoundException e) {
    throw new RuntimeException(e);
  }
}

/* (non-Javadoc)
 * @see reconcile.Document#toBlob()
 */
public static DocumentBlob toBlob(Document d)
{
  if (d instanceof DocumentBlob)
    return (DocumentBlob) d;
  else {
    DocumentBlob blob = new DocumentBlob(d.getDocumentId(), d.getText());
    blob.addMetaData(d.mMetaData);
    List<String> annotationSets = d.listAnnotationTypes();
    for (String set : annotationSets) {
      AnnotationSet a = d.getAnnotationSet(set);
      blob.addAnnotationSet(a);
    }

    return blob;
  }
}

}
