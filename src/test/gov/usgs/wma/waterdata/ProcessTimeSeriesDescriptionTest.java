package gov.usgs.wma.waterdata;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment= SpringBootTest.WebEnvironment.NONE)
public class ProcessTimeSeriesDescriptionTest {

	@MockBean
	private TimeSeriesDescriptionDao tsdDao;
	private ProcessTimeSeriesDescription processTsd;
	private RequestObject request;

	@BeforeEach
	public void beforeEach() {
		processTsd = new ProcessTimeSeriesDescription(tsdDao);
		request = new RequestObject();
		request.setId(265L);
	}

	@Test
	public void testNotFound() {
		when(tsdDao.upsertTimeSeriesDescription(anyLong())).thenReturn(Arrays.asList());
		ResultObject result = processTsd.apply(request);
		assertNotNull(result);
		List<String> uniqueIds = Arrays.asList();
		assertEquals(uniqueIds, result.getUniqueIds());
	}

	@Test
	public void testFoundGeneric() {
		List<String> expectedUniqueIds = Arrays.asList(
				"uniqueId",
				"anotherUniqueId");
		when(tsdDao.upsertTimeSeriesDescription(anyLong())).thenReturn(expectedUniqueIds);
		ResultObject result = processTsd.apply(request);
		assertNotNull(result);
		assertEquals(result.getUniqueIds(), expectedUniqueIds);
	}
}
