package orestes.bloomfilter.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
	BFTest.class,
	RedisBFTest.class })
public class AllBFTests {

}
