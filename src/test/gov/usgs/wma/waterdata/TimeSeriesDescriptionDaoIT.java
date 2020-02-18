package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
	public void testInsert() throws IOException {

		// insert new data, return unique ids
		jsonDataId.setId(265L);
		List<String> expectedIds = Arrays.asList(
				"01c56d4c5d2143f4b039e78c5f43a2d3",
				"07ac715d9db84117b2971df3d63b0837",
				"0f083f2f9dfd4cb6af6c10ca6a20c3bb",
				"10ed515c40b9430096ef44f9afdb5fe7");
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/multipleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testInsertMultiple() throws IOException {

		// insert new data, return unique ids
		jsonDataId.setId(265L);
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		List<String> expectedIds = Arrays.asList(
				"01c56d4c5d2143f4b039e78c5f43a2d3",
				"07ac715d9db84117b2971df3d63b0837",
				"0f083f2f9dfd4cb6af6c10ca6a20c3bb",
				"10ed515c40b9430096ef44f9afdb5fe7");
		assertEquals(expectedIds, actualIds);

		// insert more new data, return corresponding unique ids
		jsonDataId.setId(319L);
		actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		expectedIds = Arrays.asList(
				"016f54d5fd964c08963bc3531e185c9f",
				"080e771673ff413fa0ed4496f2b3287c",
				"1349b688bdf24c19a14ce2f6bb8da40a");
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/empty/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoInsertIfJsonDataIdNotFound() throws IOException {

		// try to upsert data using a json data id that was not found, no upsert, return no unique ids
		jsonDataId.setId(500L);
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		List<String> expectedIds = Arrays.asList();
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingOldData/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testUpdate() throws IOException {

		// update old data, return unique ids
		jsonDataId.setId(265L);
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		List<String> expectedIds = Arrays.asList(
				"01c56d4c5d2143f4b039e78c5f43a2d3",
				"07ac715d9db84117b2971df3d63b0837"
		);
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingData/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoUpdateIfSameData() throws IOException {

		// try to update data that is already current, no update, return no unique ids
		jsonDataId.setId(265L);
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		List<String> expectedIds = Arrays.asList();
		assertEquals(expectedIds, actualIds);
	}
}
