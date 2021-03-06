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
 * Created on Nov 3, 2009
 */
package reconcile.hbase.query;

import java.io.File;
import java.io.FileFilter;


/**
 * @author David Buttler
 *
 */
public class ExtensionFileFilter
    implements FileFilter {

private String mEnding;

public ExtensionFileFilter(String ending) {
  mEnding = ending;
}
/* (non-Javadoc)
 * @see java.io.FileFilter#accept(java.io.File)
 */
public boolean accept(File pathname)
{
  if (pathname.isDirectory()) return true;
  if (pathname.getName().endsWith(mEnding)) return true;
  return false;
}

}
