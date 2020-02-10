package gov.usgs.wma.waterdata;

import java.util.List;

public class ResultObject {

	private List<String> tsDescriptionUniqueIds;

	public List<String> getTsDescriptionUniqueIds() {
		return tsDescriptionUniqueIds;
	}
	public void setTsDescriptionUniqueIds(List<String> tsDescriptionIds) {
		this.tsDescriptionUniqueIds = tsDescriptionIds;
	}
}