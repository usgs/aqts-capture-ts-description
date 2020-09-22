package gov.usgs.wma.waterdata;

import java.util.ArrayList;
import java.util.List;

public class ResultObject {
	private List<TimeSeries> timeSeriesList;
	public List<TimeSeries> getTimeSeriesList() {
		return null != timeSeriesList ? timeSeriesList : new ArrayList<>();
	}
	public void setTimeSeriesList(List<TimeSeries> timeSeriesList) {
		this.timeSeriesList = timeSeriesList;
	}
}