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

	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@Test
	public void testUpsertAttemptWithBrandNewDataShouldReturn46UniqueIds() throws IOException {

		RequestObject request = new RequestObject();
		request.setId(265L);
		List<String> expectedUniqueIds = Arrays.asList(
				"01c56d4c5d2143f4b039e78c5f43a2d3",
				"07ac715d9db84117b2971df3d63b0837",
				"0f083f2f9dfd4cb6af6c10ca6a20c3bb",
				"1d88bb7935a7435fb842733cb9a3ecef",
				"1eecadfa0fec43e39f3a2bed839d0c0b",
				"22266ea03e304ac489d0efb6d3b8634c",
				"22aad04a09274845822460754930abd5",
				"25b31ba0ef564864b8600729348cae3f",
				"2b9e623b4bc047dfb6e028a9fcd12b55",
				"2d95ec9c609c40c4be99d8c97f68963d",
				"37a03870a4d74282acd810c4110a8bbe",
				"4035627d748b40f2923d8db472dfd407",
				"4071490c01834f3abcd7d861831fcef1",
				"467abcc27add41c6ab542bed22fca017",
				"4d57ec4c08f6446fb9ff31bcb4419c78",
				"598318b69c4d4d10ac177abd9d4dc5a9",
				"59c0fd556a34402499a42107bab98322",
				"5bfb676a30094f5db6783efbc540e077",
				"70faeff361ee41be8591e3ba9532107e",
				"7773e0932b9748c3a5e96f3c6b3f9140",
				"7945dd89fac846409a78ec35ee5c3362",
				"82538710d11148fc82fd74a1b386bf4e",
				"829f4b8342cf4b74b45f787a6ee87730",
				"89b8bfa683974708a764dac9c4c5fcc6",
				"8d48b829be25455a80f1d907691656f7",
				"926e80ac3e004d9f96eeb683a0f4a7ba",
				"936495d60f0543daaa197f11f1597450",
				"962150c838f344c59536f16999b5748f",
				"a3b780a70d444d92b63ae2f2cf518f98",
				"ad09cf6f24be4922b661bf4be49cb92c",
				"ad9cb97e5bc94380ababe9b96a5e3c26",
				"aec431389d60461d9fc313e34f4d0754",
				"b2992184212c4d449f2f437eb4afd684",
				"b40c95411765422bafadf06877838ec3",
				"c08d8e4b93674f87b0c2f65c53c06f13",
				"c22c5b7c624249dcb7a5c9aa5a29e1eb",
				"c2ba8785d3ec4670ba9dcb0bd528e630",
				"c4e76001721444f2844610363efe24a5",
				"d191ff6b768a490ab2d98b9d14cdceb8",
				"d1e698b0d1484513a6549ccb2e9feb8f",
				"d92ba995b33a4d21aa9d9238285eb7c5",
				"dd1e70dcfcbc4f0aaa4e0cc826115d58",
				"e5b71025784b42a48e8c2b10f6804cc9",
				"ec191bd901344505ae4ad8b0e27845ef",
				"f888a78b664a4257a981aecb8debd962",
				"feee49b5943b4994aa03c6c2905e7d1c");

		List<String> actualUniqueIds = tsdDao.upsertTimeSeriesDescriptionsForSingleJsonDataId(request.getId());
		assertEquals(expectedUniqueIds, actualUniqueIds);
	}

	@DatabaseSetup("classpath:/testData/timeSeriesDescription/empty/")
	@Test
	public void testJsonDataIdThatDoesNotHaveTimeSeriesDescriptionsReturnsEmptyList() throws IOException {

		jsonDataId.setId(500L);
		List<String> expectedUniqueIds = Arrays.asList();
		List<String> actualUniqueIds = tsdDao.upsertTimeSeriesDescriptionsForSingleJsonDataId(jsonDataId.getId());
		assertEquals(expectedUniqueIds, actualUniqueIds);
	}

	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingData/")
	@Test
	public void testUpsertAttemptsWithSameDataShouldYieldNoChangesAndReturnsEmptyList() throws IOException {

		jsonDataId.setId(265L);
		List<String> expectedUniqueIds = Arrays.asList();
		List<String> actualUniqueIds = tsdDao.upsertTimeSeriesDescriptionsForSingleJsonDataId(jsonDataId.getId());
		assertEquals(expectedUniqueIds, actualUniqueIds);
	}

	@DatabaseSetup("classpath:/testData/timeSeriesDescription/existingStaleData/")
	@Test
	public void testUpsertAttemptWithThreeRecordsThatAreNewerThanTheExistingDataShouldReturn3UniqueIds() throws IOException {

		jsonDataId.setId(265L);
		List<String> expectedUniqueIds = Arrays.asList(
				"01c56d4c5d2143f4b039e78c5f43a2d3",
				"07ac715d9db84117b2971df3d63b0837",
				"0f083f2f9dfd4cb6af6c10ca6a20c3bb"
		);
		List<String> actualUniqueIds = tsdDao.upsertTimeSeriesDescriptionsForSingleJsonDataId(jsonDataId.getId());
		assertEquals(expectedUniqueIds, actualUniqueIds);

		// A subsequent upsert with the same data should yield no changes and return an empty list of unique ids
		expectedUniqueIds = Arrays.asList();
		actualUniqueIds = tsdDao.upsertTimeSeriesDescriptionsForSingleJsonDataId(jsonDataId.getId());
		assertEquals(expectedUniqueIds, actualUniqueIds);
	}

//	@Test
//	@ExpectedDatabase(
//			connection="schema_name",
//			value="classpath:/testResult/timeSeriesDescription/happyPath/",
//			assertionMode= DatabaseAssertionMode.NON_STRICT_UNORDERED,
//			table= "time_series_description",
//			query= "upsert query goes here")
//	public void testHappyPathTableState() throws IOException {
//
//	}

}
