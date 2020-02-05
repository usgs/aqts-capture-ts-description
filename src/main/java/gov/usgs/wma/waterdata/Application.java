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
			System.out.println("Here's the input: ");
			System.out.println(value.toString());
			int tsdCount = tsdDao.getTsdRecordCount();
			int jsonDataCount = tsdDao.getJsonDataRecordCount();
			System.out.println("Here's the count from the time_series_description_table:");
			System.out.println(tsdCount);
			System.out.println("Here's the count from the json_data table:");
			System.out.println(jsonDataCount);
			return "Hello " + value.toString() + " " + tsdCount + " " + jsonDataCount;
		};
	}
}
