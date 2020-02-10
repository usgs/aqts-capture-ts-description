package gov.usgs.wma.waterdata;

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

		// The upsert returns a list of time series unique ids that were either updated or inserted.  Returns an empty
		// list if no records were updated or inserted.
		LOG.info("json_data_id: {}", request.getId());
		List<String> uniqueIds = timeSeriesDescriptionDao.upsertTimeSeriesDescriptionsForSingleJsonDataId(request.getId());
		LOG.info("uniqueIds: {}", uniqueIds);
		result.setUniqueIds(uniqueIds);
		return result;
	}
}
