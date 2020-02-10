package gov.usgs.wma.waterdata;

import java.util.List;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProcessTimeSeriesDescription implements Function<RequestObject, Object> {

	@Autowired
	TimeSeriesDescriptionDao tsdDao;

	@Override
	public  ResultObject apply(RequestObject request) {
		return processRequest(request);
	}

	protected ResultObject processRequest(RequestObject request) {
		ResultObject result = new ResultObject();

		// run the insert
		System.out.println("Here's the input: ");
		System.out.println(request.getId());
		List<String> tsduids = tsdDao.upsertTimeSeriesDescriptionsForSingleJsonDataId(request.getId());

		result.setTsDescriptionUniqueIds(tsduids);
		return result;
	}
}
