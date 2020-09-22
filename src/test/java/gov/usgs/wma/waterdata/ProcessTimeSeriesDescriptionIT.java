package gov.usgs.wma.waterdata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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

import java.util.Collections;

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

	@BeforeEach
	public void setup() {
		request = new RequestObject();
		request.setId(TimeSeriesDescriptionDaoIT.JSON_DATA_ID_265);
		request.setPartitionNumber(TimeSeriesDescriptionDaoIT.PARTITION_NUMBER);
	}

	@Test
	@DatabaseSetup("classpath:/testResult/empty/")
	@ExpectedDatabase(
			value="classpath:/testResult/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testProcessRequestSingleUpsert() {
		ResultObject result = processTsd.processRequest(request);
		assertThat(result.getTimeSeriesList(), containsInAnyOrder(
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
	public void testProcessRequestMultipleUpsert() {
		// first upsert
		ResultObject result = processTsd.processRequest(request);
		assertThat(result.getTimeSeriesList(), containsInAnyOrder(
				new TimeSeries("01c56d4c5d2143f4b039e78c5f43a2d3"),
				new TimeSeries("07ac715d9db84117b2971df3d63b0837"),
				new TimeSeries("0f083f2f9dfd4cb6af6c10ca6a20c3bb"),
				new TimeSeries("10ed515c40b9430096ef44f9afdb5fe7")
		));

		// second upsert
		request.setId(TimeSeriesDescriptionDaoIT.JSON_DATA_ID_319);
		result = processTsd.processRequest(request);
		assertThat(result.getTimeSeriesList(), containsInAnyOrder(
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
		request.setId(TimeSeriesDescriptionDaoIT.JSON_DATA_ID_500);
		ResultObject result = processTsd.processRequest(request);
		assertEquals(Collections.emptyList(), result.getTimeSeriesList());
	}

	@Test
	@DatabaseSetup("classpath:/testData/timeSeriesDescription/staleData/")
	@ExpectedDatabase(
			value="classpath:/testResult/singleUpsert/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testUpdate() {
		// update old data, return unique ids
		ResultObject result = processTsd.processRequest(request);
		assertThat(result.getTimeSeriesList(), containsInAnyOrder(
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
		ResultObject result = processTsd.processRequest(request);
		assertEquals(Collections.emptyList(), result.getTimeSeriesList());
	}

	@Test
	@DatabaseSetup("classpath:/testResult/empty/")
	@ExpectedDatabase(
			value = "classpath:/testResult/noDuplicateRowsForParmCode72019/",
			assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void testUpsertNotAffectingRowTwice() {
		request.setId(TimeSeriesDescriptionDaoIT.JSON_DATA_ID_510);
		ResultObject result = processTsd.processRequest(request);

		assertThat(result.getTimeSeriesList(), containsInAnyOrder(
				new TimeSeries("ff02b576d7d34f1c82a97e4405b16384"),
				new TimeSeries("ed013f578a924df090ead63f05c5e5cc"),
				new TimeSeries("739c687d86ee45308d09e9c8007f72b3"),
				new TimeSeries("398ca443b25842a0a07d0e5741d25303"),
				new TimeSeries("29be33c9a1ab4fa1aafb0ed97f2307ad"),
				new TimeSeries("0f1ca4474f124e4ca3407e2aaa418c1c")
				)
		);

		// This tests that the below error no longer occurs (was due to duplicate
		// parm_cd in aq_comp_id_to_stat_cd table).
		// ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time
		assertDoesNotThrow(() -> {
			processTsd.processRequest(request);
		}, "should not have thrown an exception but did");
	}
}
