package hyren.serv6.h;

import hyren.serv6.h.process.run.ProcessMainJob;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(excludeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = { ProcessMainJob.class }), @ComponentScan.Filter(type = FilterType.REGEX, pattern = "hyren.serv6.h.process_flink.*") })
public class AppMain {

    public static void main(String[] args) {
        SpringApplication.run(AppMain.class, args);
    }
}
