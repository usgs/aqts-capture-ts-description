package gov.usgs.wma.waterdata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

@SpringBootTest(webEnvironment=WebEnvironment.NONE,
		classes={DBTestConfig.class, TimeSeriesDescriptionDao.class})
@DatabaseSetup("classpath:/testData/jsonData/")

@ActiveProfiles("it")
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class,
		DirtiesContextTestExecutionListener.class,
		TransactionalTestExecutionListener.class,
		TransactionDbUnitTestExecutionListener.class })
@DbUnitConfiguration(dataSetLoader=FileSensingDataSetLoader.class)
@AutoConfigureTestDatabase(replace=Replace.NONE)
@Transactional(propagation=Propagation.NOT_SUPPORTED)
@Import({DBTestConfig.class})
@DirtiesContext
public class TimeSeriesDescriptionDaoIT {

	protected static final Long JSON_DATA_ID_1 = 1L;
	protected static final Long JSON_DATA_ID_265 = 265L;
	protected static final Long JSON_DATA_ID_319 = 319L;
	protected static final Long JSON_DATA_ID_500 = 500L;
	protected static final Long JSON_DATA_ID_510 = 510L;
	protected static final Integer PARTITION_NUMBER = 7;

	@Autowired
	private TimeSeriesDescriptionDao tsdDao;
	private RequestObject request = new RequestObject();

	@BeforeEach
	public void beforeEach() {
		request = new RequestObject();
		request.setId(JSON_DATA_ID_265);
		request.setPartitionNumber(PARTITION_NUMBER);
	}

	@Test
	@DatabaseSetup("classpath:/testResult/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testInsert() {
		// insert new data, return unique ids
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(request);
		assertThat(actualIds, containsInAnyOrder(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
				));
	}

	@Test
	@DatabaseSetup("classpath:/testResult/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/multipleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testInsertMultiple() {
		// insert new data, return unique ids
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(request);
		assertThat(actualIds, containsInAnyOrder(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
				));

		// insert more new data, return corresponding unique ids
		request.setId(JSON_DATA_ID_319);
		actualIds = tsdDao.upsertTimeSeriesDescription(request);
		assertThat(actualIds, containsInAnyOrder(
				new TimeSeries("016f54d5fd964c08963bc3531e185c9f"),
				new TimeSeries("080e771673ff413fa0ed4496f2b3287c"),
				new TimeSeries("1349b688bdf24c19a14ce2f6bb8da40a")
				));
	}

	@Test
	@DatabaseSetup("classpath:/testResult/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/empty/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoInsertIfJsonDataIdNotFound() {
		// try to upsert data using a json data id that was not found, no upsert, return no unique ids
		request.setId(JSON_DATA_ID_500);
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(request);
		assertTrue(actualIds.isEmpty());
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/staleData/")
	@ExpectedDatabase(
			value="classpath:/testResult/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testUpdate() {
		// update old data, return unique ids
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(request);
		assertThat(actualIds, containsInAnyOrder(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
				));
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/currentData/")
	@ExpectedDatabase(
			value="classpath:/testResult/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoUpdateIfSameData() {
		// try to update data that is already current, no update, return no unique ids
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(request);
		assertTrue(actualIds.isEmpty());
	}

	@Test
	@DatabaseSetup("classpath:/testResult/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/handleMicroseconds/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testMicrosecondHandling() {
		// This is to ensure we handle fractional seconds on timestamps appropriately
		request.setId(JSON_DATA_ID_1);
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(request);
		assertThat(actualIds, containsInAnyOrder(
				new TimeSeries("testId1"),
				new TimeSeries("testId2"),
				new TimeSeries("testId3"),
				new TimeSeries("testId4")
				));
	}

	@Test
	@DatabaseSetup("classpath:/testResult/empty/")
	@ExpectedDatabase(value = "classpath:/testResult/noSecondRows/", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testUpsertNotAffectingSecondRow() {
		// This tests that the below error no longer occurs (was due to duplicate
		// parm_cd in aq_comp_id_to_stat_cd table).
		// ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time
		request.setId(JSON_DATA_ID_510);
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(request);
		System.out.println("actual ids len = " + actualIds.size());
		System.out.println("actual ids = " + actualIds);
//		assertThat(actualIds, containsInAnyOrder(new TimeSeries("testId1"), new TimeSeries("testId2"),
//				new TimeSeries("testId3"), new TimeSeries("testId4")));
	}
}
