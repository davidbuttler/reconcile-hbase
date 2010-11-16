/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package reconcile.hbase.mapreduce.annotation;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

/**
 * Thread which periodically reports to a hadoop {@link Reporter}. This is needed for long taking map/reduce-task which
 * do not write to the output collector, since hadoop kills task which do not write output or report their status for a
 * configured amount of time.
 */

@SuppressWarnings("rawtypes")
public class ReportProgressThread
    extends Thread {

protected final static Logger LOG = Logger.getLogger(ReportProgressThread.class);

private final Context _reporter;

private final long _reportIntervall;

public ReportProgressThread(Context context, long reportIntervall) {
  _reporter = context;
  _reportIntervall = reportIntervall;
  setDaemon(true);
  _reporter.getCounter("report progress thread", "started").increment(1);
}


@Override
public void run()
{
  try {
    while (true) {
      _reporter.progress();
      _reporter.getCounter("report progress thread", "tick").increment(1);
      sleep(_reportIntervall);
    }
  }
  catch (final InterruptedException e) {
    LOG.debug("progress thread stopped");
  }
  _reporter.getCounter("report progress thread", "completed").increment(1);
}

public static ReportProgressThread start(Context reporter, long reportIntervall)
{
  ReportProgressThread reportStatusThread = new ReportProgressThread(reporter, reportIntervall);
  reportStatusThread.start();
  return reportStatusThread;
}

}
