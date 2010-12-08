package reconcile.hbase.mapreduce;

import java.util.Set;

import junit.framework.Assert;
import junit.framework.TestCase;

public class JobConfigTest extends TestCase
{
	static String FOO="foo";
	static String BAR="bar";
	static String FOOBAR=FOO+BAR;

	public void testSourceArg()
	{
		String[] args = { JobConfig.SOURCE_ARG+FOO };
		JobConfig jobConfig = new JobConfig(args);
		Assert.assertEquals(FOO, jobConfig.getSource());
	}

	public void testKeyListArg()
	{
		String[] args = { JobConfig.KEY_LIST_ARG+FOO };
		JobConfig jobConfig = new JobConfig(args);
		Assert.assertEquals(FOO, jobConfig.getKeyListFile());
	}

	public void testTableArg()
	{
		String[] args = { JobConfig.TABLE_ARG+FOO };
		JobConfig jobConfig = new JobConfig(args);
		Assert.assertEquals(FOO, jobConfig.getTableName());
	}

	public void testAllArgs()
	{
		String[] args = { JobConfig.SOURCE_ARG+FOO,
				JobConfig.KEY_LIST_ARG+BAR,
				JobConfig.TABLE_ARG+FOOBAR
		};

		JobConfig jobConfig = new JobConfig(args);

		Assert.assertEquals(FOO, jobConfig.getSource());
		Assert.assertEquals(BAR, jobConfig.getKeyListFile());
		Assert.assertEquals(FOOBAR, jobConfig.getTableName());
	}

	public void testAllButTableArgs()
	{
		String[] args = { JobConfig.SOURCE_ARG+FOO,
				JobConfig.KEY_LIST_ARG+BAR,
		};

		JobConfig jobConfig = new JobConfig(args);

		Assert.assertEquals(FOO, jobConfig.getSource());
		Assert.assertEquals(BAR, jobConfig.getKeyListFile());

		// Ensure table is source
		Assert.assertEquals(FOO, jobConfig.getTableName());
	}
	
	private static final String PRE1="-prefix1=";
	private static final String PRE2="-prefix2=";
	private static final String PRE3="-prefix3=";
	private static final String FIRST_VALUE="good";
	public void testFirstArg()
	{	
		String[] args = { PRE1+FIRST_VALUE, PRE1+"better", PRE2+FIRST_VALUE, PRE2+"better" };

		JobConfig jc = new JobConfig(args);
		Assert.assertEquals(FIRST_VALUE, jc.getFirstArg(PRE1));
		Assert.assertEquals(FIRST_VALUE, jc.getFirstArg(PRE2));
		Assert.assertNull(jc.getFirstArg(PRE3));
	}

public void testGetArg()
{
  String[] args = { "-prefix1=good", "-prefix1=better", "-prefix1=best", "-prefix2=good", "-prefix2=good" };
  JobConfig jc = new JobConfig(args);
  Set<String> p1 = jc.getArg("-prefix1=");
  assertTrue(p1.size() == 3);
  assertTrue(p1.contains("good"));
  assertTrue(p1.contains("better"));
  assertTrue(p1.contains("best"));

  Set<String> p2 = jc.getArg("-prefix2=");
  assertTrue(p2.size() == 1);
  assertTrue(p2.contains("good"));

}
}
