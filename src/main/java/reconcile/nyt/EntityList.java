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
 * Created on Apr 8, 2008
 * 
 * $Id: EntityList.java,v 1.3 2009/05/14 00:45:20 buttler1 Exp $
 */
package reconcile.nyt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import gov.llnl.text.util.FileUtils;
import gov.llnl.text.util.InputStreamLineIterable;

/**
 * Wraps an entity to id map, backed by a file
 * 
 * @author David Buttler
 * 
 */
public class EntityList
    implements Map<Integer, String> {

private static Pattern pSpace = Pattern.compile("\\s+");

private boolean mPreserveCase;

private File mFile;

private int mId = 0;

private TreeMap<String, Integer> entities;

private HashMap<Integer, String> ids;

private PrintWriter mOut;

/*
 * this variable determines if anything will be allowed to affect the file system
 * if readOnly is true, then any method that would update the file system should throw a runtime exception
 */
private boolean mReadOnly = false;

/**
 * Create a new list of strings with associated id, backed by a given file. <br>
 * if the file already contains such a list, read it in first
 * 
 * @param f
 * @throws IOException
 */
public EntityList(File f)
    throws IOException {
  this(f, false, false);
}

/**
 * Create a new list of strings with associated id, backed by a given file. <br>
 * if the file already contains such a list, read it in first
 * 
 * @param f
 * @param readOnly
 *          should the list be read only
 * @throws IOException
 */
public EntityList(File f, boolean readOnly)
    throws IOException {
  this(f, false, readOnly);
}

/**
 * Create a new list of strings with associated id, backed by a given file. <br>
 * if the file already contains such a list, read it in first
 * 
 * @param f
 * @param preserveCase
 *          should the list preserve case of the added entities
 * @param readOnly
 *          should the list be read only -- should not be true if preserve case is true, else what's the point? However,
 *          both are needed to distinguish the different constructors
 * @throws IOException
 */
public EntityList(File f, boolean readOnly, boolean preserveCase)
    throws IOException {
  mFile = f;
  mPreserveCase = preserveCase;
  mReadOnly = readOnly;
  entities = new TreeMap<String, Integer>();
  ids = new HashMap<Integer, String>();
  if (mFile.exists() && mFile.length() > 0) {
    readFile();
  }
  if (!mReadOnly) {
    mOut = FileUtils.compressedOut(f);
    rewrite();
  }
}

public void setPreserveCase(boolean v)
{
  mPreserveCase = v;
}

public boolean isPreserveCase()
{
  return mPreserveCase;

}

public Set<Integer> idSet()
{
  return Collections.unmodifiableSet(ids.keySet());
}

public Set<String> valueSet()
{
  return Collections.unmodifiableSet(entities.keySet());
}

public int size()
{
  assert (entities.size() == ids.size());
  return entities.size();
}

private void readFile()
    throws IOException
{

  synchronized (entities) {
    for (String line : InputStreamLineIterable.iterate(new GZIPInputStream(new FileInputStream(mFile)))) {
      line = line.trim();
      Matcher m = pSpace.matcher(line);
      boolean s = m.find();
      if (s) {
        Integer id = Integer.parseInt(line.substring(0, m.start()));
        String str = normalizeText(line.substring(m.end()));
        entities.put(str, id);
        ids.put(id, str);
        if (id >= mId) {
          mId = id + 1;
        }
      }

    }
  }
}

public int getId(String s)
{
  s = normalizeText(s);
  synchronized (entities) {
    Integer r = entities.get(s);
    if (r != null) return r;
    return -1;
  }
}

public String getString(int id)
{
  return ids.get(id);

}

/**
 * Add a string to the list. An id will be automatically assigned.
 * 
 * @param s
 * @return
 */
public int add(String str)
{
  if (mReadOnly) throw new RuntimeException("list is read only");
  str = cleanText(str);
  String s = normalizeText(str);
  synchronized (entities) {
    if (entities.get(s) == null) {
      int id = mId++;
      entities.put(s, id);
      ids.put(id, str);
      String out = id + " " + str;
      mOut.println(out);
      return id;
    }
    else
      return entities.get(s);
  }
}

/**
 * Set the id of a given string<br>
 * Note that the string will be normalized
 * 
 * @param id
 * @param str
 */
public void set(int id, String str)
{
  if (mReadOnly) throw new RuntimeException("list is read only");
  synchronized (entities) {
    String existing = ids.get(id);
    if (existing != null) throw new RuntimeException("id already exists");
    if (id > mId) {
      mId = id + 1;
    }
    str = cleanText(str);
    String s = normalizeText(str);
    ids.put(id, str);
    entities.put(s, id);
    String out = id + " " + str;
    mOut.println(out);
  }
}

public void close()
{
  if (!mReadOnly) {
    mOut.flush();
    mOut.close();
  }
}

/**
 * Rewrites the entity map in sorted order (sorted by entity lexigraphical sort)
 * 
 * @throws IOException
 */
public void sort()
    throws IOException
{
  if (mReadOnly) throw new RuntimeException("list is read only");
  mOut.flush();
  mOut.close();
  mOut = FileUtils.compressedOut(mFile);

  rewrite();
}

private void rewrite()
{
  if (mReadOnly) throw new RuntimeException("list is read only");
  synchronized (entities) {
    for (Map.Entry<String, Integer> line : entities.entrySet()) {
      mOut.println(line.getValue() + " " + line.getKey());
    }
  }
}

/**
 * @param s
 * @return
 */
public String normalizeText(String s)
{
  if (!mPreserveCase) {
    s = s.toLowerCase();
  }
  s = pSpace.matcher(s).replaceAll(" ");
  s = s.trim();
  return s;
}

/**
 * @param s
 * @return
 */
public String cleanText(String s)
{
  s = pSpace.matcher(s).replaceAll(" ");
  s = s.trim();
  return s;
}

/**
 * @param s
 * @return
 */
public static String normalize(String s)
{
  s = s.toLowerCase();
  s = pSpace.matcher(s).replaceAll(" ");
  s = s.trim();
  return s;
}

public static void main(String[] args)
{
  File f = new File(args[0]);
  try {
    EntityList list = new EntityList(f);
    list.sort();
    list.close();
  }
  catch (IOException e) {
    e.printStackTrace();
  }

}

/* (non-Javadoc)
 * @see java.lang.Object#finalize()
 */
@Override
protected void finalize()
    throws Throwable
{
  super.finalize();
  close();
}

/**
 * Given another EntityList, ensure the current entity list contains all of the strings of the other one, <br>
 * and create a mapping between the other id set an my id set.
 * 
 * @param other
 * @return map from the other ids to my ids
 */
public Map<Integer, Integer> map(EntityList other)
{
  Map<Integer, Integer> map = new TreeMap<Integer, Integer>();
  for (String s : other.entities.keySet()) {
    int oId = other.entities.get(s);
    int myId = add(s);
    map.put(oId, myId);
  }
  return map;
}

/* (non-Javadoc)
 * @see java.util.Map#clear()
 */
public void clear()
{
  throw new RuntimeException("operation not supported");

}

/* (non-Javadoc)
 * @see java.util.Map#containsKey(java.lang.Object)
 */
public boolean containsKey(Object key)
{
  return ids.containsKey(key);
}

/* (non-Javadoc)
 * @see java.util.Map#containsValue(java.lang.Object)
 */
public boolean containsValue(Object value)
{
  return ids.containsValue(value);
}

/* (non-Javadoc)
 * @see java.util.Map#entrySet()
 */
public Set<java.util.Map.Entry<Integer, String>> entrySet()
{
  return ids.entrySet();
}

/* (non-Javadoc)
 * @see java.util.Map#get(java.lang.Object)
 */
public String get(Object key)
{
  return ids.get(key);
}

/* (non-Javadoc)
 * @see java.util.Map#isEmpty()
 */
public boolean isEmpty()
{
  return ids.isEmpty();
}

/* (non-Javadoc)
 * @see java.util.Map#keySet()
 */
public Set<Integer> keySet()
{
  return ids.keySet();
}

/* (non-Javadoc)
 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
 */
public String put(Integer key, String value)
{
  set(key, value);
  return value;
}

/* (non-Javadoc)
 * @see java.util.Map#putAll(java.util.Map)
 */
public void putAll(Map<? extends Integer, ? extends String> m)
{
  for (int id : m.keySet()) {
    String val = m.get(id);
    set(id, val);
  }
}

/* (non-Javadoc)
 * @see java.util.Map#remove(java.lang.Object)
 */
public String remove(Object key)
{
  throw new RuntimeException("operation not supported");
}

/* (non-Javadoc)
 * @see java.util.Map#values()
 */
public Collection<String> values()
{
  return ids.values();
}
}
