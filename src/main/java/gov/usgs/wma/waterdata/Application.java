package gov.usgs.wma.waterdata;

import java.util.function.Function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Application {

	@Autowired
	TimeSeriesDescriptionDao tsdDao;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public Function<Object, String> echoObject() {
		return value -> {
			// The input
			System.out.println("Here's the input: ");
			System.out.println(value.toString());

			// Run a count of records that will be inserted
			int timeSeriesDescriptionsCount = tsdDao.getTimeSeriesDescriptionsForSingleJsonDataIdCount();
			System.out.println("Here's the count from the timeSeriesDescriptions query to the json_data table:");
			System.out.println(timeSeriesDescriptionsCount);

			// Run an insert into the time_series_description table
			tsdDao.insertTimeSeriesDescriptionsForSingleJsonDataId();

			return "Hello " + value.toString() + " " + timeSeriesDescriptionsCount;
		};
	}
}
