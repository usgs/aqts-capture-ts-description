package gov.usgs.wma.waterdata;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

@SpringBootTest(webEnvironment=WebEnvironment.NONE,
		classes={DBTestConfig.class, TimeSeriesDescriptionDao.class})
@DatabaseSetup("classpath:/testData/")

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

public class timeSeriesDescriptionDaoIT {

	@Autowired
	private TimeSeriesDescriptionDao tsdDao;

	@Test
	public void foundTest() {
		assertEquals(0, tsdDao.upsertTimeSeriesDescriptionsForSingleJsonDataId());
	}

}
