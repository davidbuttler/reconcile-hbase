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
 * $Id: MemoryFileSystem.java,v 1.3 2009/05/14 00:45:20 buttler1 Exp $
 */
package reconcile.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * The current version stores ByteArrayOutputStreams instead of byte arrays so that we can get some
 *  benefits of being able to write to an output stream up until the point it is
 *  either serialized or the results are retrieved.  The downside is that a file could change
 *  after you read it.  
 * @author David Buttler
 * 
 */
public class MemoryFileSystem
    implements Serializable {

/**
   * 
   */
  private static final String CURRENT_VERSION = "1.5.1";
/**
   * 
   */
private static final long serialVersionUID = 1L;
Map<String, ByteArrayOutputStream> fs;

public MemoryFileSystem() {
  fs = Maps.newHashMap();
}

public boolean delete(File f)
{
  String path = f.getPath();
  ByteArrayOutputStream result = fs.remove(path);
  if (result == null) return false;
  return true;
}

public boolean delete(String path)
{
  ByteArrayOutputStream result = fs.remove(path);
  if (result == null) return false;
  return true;
}

public byte[] get(File f)
{
  String path = f.getPath();
  ByteArrayOutputStream baos = fs.get(path);
  if (baos == null) {
    return null;
  }
  return baos.toByteArray();
}

public byte[] get(String path)
{
  ByteArrayOutputStream baos = fs.get(path);
  if (baos == null) {
    return null;
  }
  return baos.toByteArray();
}

/**
 * @param name
 * @param ba
 */
public void put(String name, ByteArrayOutputStream ba)
{
  fs.put(name, ba);
}

/**
 * @return
 */
public Iterable<String> list()
{
  return fs.keySet();
}

/**
 * Serialize to an older version of the Memory File System (1.5.0)
 * @param out
 * @throws IOException
 */
public void serializeOut(ObjectOutputStream out) throws IOException {
  out.writeInt(fs.size());
  for (String key : fs.keySet()) {
    out.writeObject(key);
    byte[] buf = fs.get(key).toByteArray();
    out.writeObject(buf);
  }
  
}
private void writeObject(ObjectOutputStream out)
    throws IOException
{
  out.writeInt(-1);
  out.writeObject(CURRENT_VERSION);
  out.writeObject(Integer.valueOf(fs.size()));
  for (String key : fs.keySet()) {
    out.writeObject(key);
    byte[] buf = fs.get(key).toByteArray();
    out.writeObject(buf);
  }  
}

private void readObject(ObjectInputStream in)
    throws IOException, ClassNotFoundException
{
  deserialize(in);
}
/**
 * Given an stream which this has been serialized to, deserialize.  Supports versions starting with 1.50
 * @param in
 * @throws IOException
 * @throws ClassNotFoundException
 */
public void deserialize(ObjectInputStream in) throws IOException, ClassNotFoundException {
  int size = in.readInt();
  if (size <=0) {
    Object o = in.readObject();
    if (o instanceof String) {
      deserialize(in, (String)o);
    }
    else {
      throw new IOException("Object type not recognized as Memory File System: "+o.getClass());
    }
  }
  else {
    deserialize(in, size);
  }
}


/**
 * Deserialize version 1.5.1
 * @param in
 * @param size
 * @throws IOException
 * @throws ClassNotFoundException
 */
private void deserialize(ObjectInputStream in, String version) throws IOException, ClassNotFoundException {
  
  if (version.equals(CURRENT_VERSION)) {
    int size = ((Integer)in.readObject()).intValue();
    fs = new HashMap<String, ByteArrayOutputStream>((size * 4) / 3);
    for (int i = 0; i < size; i++) {
      String key = (String) in.readObject();
      byte[] buf = (byte[]) in.readObject();
      ByteArrayOutputStream out = new ByteArrayOutputStream(buf.length);
      out.write(buf);
      fs.put(key, out);
    }
  }
  else {
    throw new IOException("version: "+version+" not supported");
  }
}

/**
 * Deserialize version 1.5.0
 * @param in
 * @param size
 * @throws IOException
 * @throws ClassNotFoundException
 */
private void deserialize(ObjectInputStream in, int size) throws IOException, ClassNotFoundException {
  fs = new HashMap<String, ByteArrayOutputStream>((size * 4) / 3);
  for (int i = 0; i < size; i++) {
    String key = (String) in.readObject();
    byte[] buf = (byte[]) in.readObject();
    ByteArrayOutputStream out = new ByteArrayOutputStream(buf.length);
    out.write(buf);
    fs.put(key, out);
  }
}
}
