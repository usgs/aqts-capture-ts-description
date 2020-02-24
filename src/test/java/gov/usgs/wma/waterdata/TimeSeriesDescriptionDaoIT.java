package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

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

	@Autowired
	private TimeSeriesDescriptionDao tsdDao;
	private RequestObject jsonDataId = new RequestObject();

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testInsert() {

		// insert new data, return unique ids
		jsonDataId.setId(265L);
		List<TimeSeries> expectedIds = Arrays.asList(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
				);
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/multipleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testInsertMultiple() {

		// insert new data, return unique ids
		jsonDataId.setId(265L);
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		List<TimeSeries> expectedIds = Arrays.asList(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
				);
		assertEquals(expectedIds, actualIds);

		// insert more new data, return corresponding unique ids
		jsonDataId.setId(319L);
		actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		expectedIds = Arrays.asList(
				new TimeSeries("016f54d5fd964c08963bc3531e185c9f"),
				new TimeSeries("080e771673ff413fa0ed4496f2b3287c"),
				new TimeSeries("1349b688bdf24c19a14ce2f6bb8da40a")
				);
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/empty/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoInsertIfJsonDataIdNotFound() {

		// try to upsert data using a json data id that was not found, no upsert, return no unique ids
		jsonDataId.setId(500L);
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		List<TimeSeries> expectedIds = Arrays.asList();
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingOldData/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testUpdate() {

		// update old data, return unique ids
		jsonDataId.setId(265L);
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		List<TimeSeries> expectedIds = Arrays.asList(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837")
				);
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingData/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoUpdateIfSameData() {

		// try to update data that is already current, no update, return no unique ids
		jsonDataId.setId(265L);
		List<TimeSeries> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		List<TimeSeries> expectedIds = Arrays.asList();
		assertEquals(expectedIds, actualIds);
	}
}
