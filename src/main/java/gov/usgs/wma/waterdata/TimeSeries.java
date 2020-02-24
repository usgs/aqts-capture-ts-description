package gov.usgs.wma.waterdata;

import java.util.Objects;

public class TimeSeries {
	private String uniqueId;
	public TimeSeries(String uniqueId) {
		this.uniqueId = uniqueId;
	}
	public String getUniqueId() {
		return uniqueId;
	}
	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}
	@Override
	public boolean equals(Object o) {
		if (o == this) return true;
		if (!(o instanceof TimeSeries)) {
			return false;
		}
		TimeSeries timeseries = (TimeSeries) o;
		return Objects.equals(uniqueId, timeseries.uniqueId);
	}
	@Override
	public int hashCode() {
		return Objects.hash(uniqueId);
	}
}
