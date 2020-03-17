package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;
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

import javax.xml.transform.Result;
import java.util.Arrays;
import java.util.List;

@SpringBootTest(webEnvironment=WebEnvironment.NONE,
		classes={
				DBTestConfig.class,
				TimeSeriesDescriptionDao.class,
				ProcessTimeSeriesDescription.class})
@DatabaseSetup("classpath:/testData/jsonData/")

@ActiveProfiles("it")
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class,
		DirtiesContextTestExecutionListener.class,
		TransactionalTestExecutionListener.class,
		TransactionDbUnitTestExecutionListener.class })
@DbUnitConfiguration(
		dataSetLoader=FileSensingDataSetLoader.class)
@AutoConfigureTestDatabase(replace=Replace.NONE)
@Transactional(propagation=Propagation.NOT_SUPPORTED)
@Import({DBTestConfig.class})
@DirtiesContext
public class ProcessTimeSeriesDescriptionIT {

	@Autowired
	public ProcessTimeSeriesDescription processTsd;

	public RequestObject request;
	public static final Long tsUniqueId = 265L;

	@BeforeEach
	public void setup() {
		request = new RequestObject();
		request.setId(tsUniqueId);
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testProcessRequestSingleUpsert() {
		ResultObject result = processTsd.processRequest(request);
		List<TimeSeries> expectedIds = Arrays.asList(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
		);
		assertEquals(expectedIds, result.getTimeSeriesList());
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/multipleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testProcessRequestMultipleUpsert() {
		// first upsert
		ResultObject result = processTsd.processRequest(request);
		List<TimeSeries> expectedIds = Arrays.asList(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
		);
		assertEquals(expectedIds, result.getTimeSeriesList());

		// second upsert
		request.setId(319L);
		result = processTsd.processRequest(request);
		expectedIds = Arrays.asList(
				new TimeSeries("016f54d5fd964c08963bc3531e185c9f"),
				new TimeSeries("080e771673ff413fa0ed4496f2b3287c"),
				new TimeSeries("1349b688bdf24c19a14ce2f6bb8da40a")
		);
		assertEquals(expectedIds, result.getTimeSeriesList());
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/empty/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoInsertIfJsonDataIdNotFound() {
		// try to upsert data using a json data id that was not found, no upsert, return no unique ids
		request.setId(500L);
		ResultObject result = processTsd.processRequest(request);
		List<TimeSeries> expectedIds = Arrays.asList();
		assertEquals(expectedIds, result.getTimeSeriesList());
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingOldData/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testUpdate() {
		// update old data, return unique ids
		request.setId(265L);
		ResultObject result = processTsd.processRequest(request);
		List<TimeSeries> expectedIds = Arrays.asList(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
		);
		assertEquals(expectedIds, result.getTimeSeriesList());
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingData/")
	@ExpectedDatabase(
			value="classpath:/testResult/timeSeriesDescription/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testNoUpdateIfSameData() {
		// try to update data that is already current, no update, return no unique ids
		ResultObject result = processTsd.processRequest(request);
		List<TimeSeries> expectedIds = Arrays.asList();
		assertEquals(expectedIds, result.getTimeSeriesList());
	}
}
