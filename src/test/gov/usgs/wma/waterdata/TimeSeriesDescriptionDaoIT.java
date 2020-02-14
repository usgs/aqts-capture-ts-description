package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesDescriptionDaoIT.class);

	@Autowired
	private TimeSeriesDescriptionDao tsdDao;
	private RequestObject jsonDataId = new RequestObject();

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testInsert() throws IOException {

		jsonDataId.setId(265L);
		List<String> expectedIds = Arrays.asList(
				"01c56d4c5d2143f4b039e78c5f43a2d3",
				"07ac715d9db84117b2971df3d63b0837",
				"0f083f2f9dfd4cb6af6c10ca6a20c3bb");
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/multipleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testInsertMultiple() throws IOException {

		jsonDataId.setId(265L);
		tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());

		jsonDataId.setId(319L);
		List<String> expectedIds = Arrays.asList(
				"016f54d5fd964c08963bc3531e185c9f",
				"080e771673ff413fa0ed4496f2b3287c");

		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/empty/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoInsertIfBadJsonDataId() throws IOException {

		jsonDataId.setId(500L);
		List<String> expectedIds = Arrays.asList();
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingOldData/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testUpdate() throws IOException {

		jsonDataId.setId(265L);
		List<String> expectedIds = Arrays.asList(
				"01c56d4c5d2143f4b039e78c5f43a2d3",
				"07ac715d9db84117b2971df3d63b0837"
		);
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		assertEquals(expectedIds, actualIds);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingData/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoUpdateIfSameData() throws IOException {

		jsonDataId.setId(265L);

		List<String> expectedIds = Arrays.asList();
		List<String> actualIds = tsdDao.upsertTimeSeriesDescription(jsonDataId.getId());
		assertEquals(expectedIds, actualIds);
	}
}
