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
 * $Id: MemoryEntityList.java,v 1.3 2009/05/14 00:45:20 buttler1 Exp $
 */
package reconcile.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Wraps an entity to id map, backed by a file
 * 
 * @author David Buttler
 * 
 */
public class MemoryEntityList
    implements Map<Integer, String> {

private static Pattern pSpace = Pattern.compile("\\s+");

private boolean mPreserveCase;

private int mId = 0;

private TreeMap<String, Integer> entities;

private HashMap<Integer, String> ids;

/**
 * Create a new list of strings with associated id, backed by a given file. <br>
 * if the file already contains such a list, read it in first
 * 
 * @param f
 * @throws IOException
 */
public MemoryEntityList() {
  this(false);
}

/**
 * Create a new list of strings with associated id, backed by a given file. <br>
 * if the file already contains such a list, read it in first
 * 
 * @param preserveCase
 *          should the list preserve case of the added entities
 */
public MemoryEntityList(boolean preserveCase) {
  mPreserveCase = preserveCase;
  entities = new TreeMap<String, Integer>();
  ids = new HashMap<Integer, String>();
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
  str = cleanText(str);
  String s = normalizeText(str);
  synchronized (entities) {
    if (entities.get(s) == null) {
      int id = mId++;
      entities.put(s, id);
      ids.put(id, str);
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

/**
 * Given another EntityList, ensure the current entity list contains all of the strings of the other one, <br>
 * and create a mapping between the other id set an my id set.
 * 
 * @param other
 * @return map from the other ids to my ids
 */
public Map<Integer, Integer> map(MemoryEntityList other)
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
