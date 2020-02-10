package gov.usgs.wma.waterdata;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProcessTimeSeriesDescription implements Function<RequestObject, Object> {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessTimeSeriesDescription.class);

	@Autowired
	TimeSeriesDescriptionDao timeSeriesDescriptionDao;

	@Override
	public  ResultObject apply(RequestObject request) {
		return processRequest(request);
	}

	protected ResultObject processRequest(RequestObject request) {

		ResultObject result = new ResultObject();
		LOG.info("processing json_data_id: {}", request.getId());

		try {
			List<String> uniqueIds = timeSeriesDescriptionDao.upsertTimeSeriesDescriptionsForSingleJsonDataId(request.getId());
			result.setUniqueIds(uniqueIds);
			LOG.info("updated or inserted uniqueIds: {}", result.getUniqueIds());
		} catch (IOException e) {
			LOG.error(e.getLocalizedMessage());
		}

		return result;
	}
}
