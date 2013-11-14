package orestes.bloomfilter.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
	BFTests.class,
	RedisBFTests.class,
    BFPopulationTests.class,
    RedisBFPopulationTests.class })
public class AllBFTests {

}
