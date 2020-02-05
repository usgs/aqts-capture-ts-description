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
//			System.out.println("Here's the input: ");
//			System.out.println(value.toString());
			int count = tsdDao.getRecordCount();
			System.out.println("Here's the count:");
			System.out.println(count);
			return "Hello " + value + count;
		};
	}
}
