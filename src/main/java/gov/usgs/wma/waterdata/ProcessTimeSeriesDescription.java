package gov.usgs.wma.waterdata;

import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProcessTimeSeriesDescription implements Function<RequestObject, ResultObject> {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessTimeSeriesDescription.class);

	private TimeSeriesDescriptionDao tsdDao;

	@Autowired
	public ProcessTimeSeriesDescription(TimeSeriesDescriptionDao tsdDao) {
		this.tsdDao = tsdDao;
	}

	@Override
	public  ResultObject apply(RequestObject request) {
		return processRequest(request);
	}

	protected ResultObject processRequest(RequestObject request) {

		Long jsonDataId = request.getId();
		ResultObject result = new ResultObject();

		if (null != jsonDataId) {
			List<TimeSeries> timeSeriesList = tsdDao.upsertTimeSeriesDescription(jsonDataId, request.getPartitionNumber());
			result.setTimeSeriesList(timeSeriesList);
			LOG.debug("Successfully processed json data id: {} and upserted unique ids: {}", jsonDataId, result.getTimeSeriesList());
		}
		return result;
	}
}
