package hyren.serv6.agent;

import hyren.serv6.agent.run.CollectJobCommand;
import hyren.serv6.agent.run.SemiStructuredJobCommand;
import hyren.serv6.agent.run.UnstructuredJobCommand;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = { CollectJobCommand.class, UnstructuredJobCommand.class, SemiStructuredJobCommand.class }))
public class AppMain {

    public static void main(String[] args) {
        SpringApplication.run(AppMain.class, args);
    }
}
