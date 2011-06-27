/*
 * Copyright (c) 2008, Lawrence Livermore National Security, LLC. Produced at the Lawrence Livermore National
 * Laboratory. Written by David Buttler, buttler1@llnl.gov
 */

package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.EnglishStemmer;

import gov.llnl.text.util.InputStreamLineIterable;

/**
 * A lookup table for stop words
 * 
 * @author buttler
 */
public class StopWord {

public static final Pattern pWord = Pattern.compile("(\\w|[-])+");

private Set<String> mStopWords;

private SnowballProgram mStemmer;

/**
 * 
 */
protected StopWord() {
  mStemmer = new EnglishStemmer();
}

/**
 * @param fileName
 *          name of stopword file
 * @throws IOException
 */
public StopWord(InputStream is)
    throws IOException {
  this();

  readStopWords(is);
}

/**
 * @param fileName
 *          name of stopword file
 * @throws IOException
 */
public StopWord(File file)
    throws IOException {
  this();

  readStopWords(new FileInputStream(file));
}


private void readStopWords(InputStream file)
{
  mStopWords = new HashSet<String>();

  try {
    for (String line : InputStreamLineIterable.iterateOverCommentedLines(file)) {
      line = stem(line);
      line = line.toLowerCase();
      mStopWords.add(line);
    }
  }
  catch (Exception e) {
    System.out.println(e.getMessage());
  }
}

public boolean isStopWord(String token)
{
  token = stem(token);
  token = token.toLowerCase();
  if (mStopWords.contains(token)) return true;
  if (!pWord.matcher(token).matches()) return true;
  return false;
}

public String stem(String token)
{
  mStemmer.setCurrent(token);
  mStemmer.stem();
  token = mStemmer.getCurrent();

  return token;
}
}
