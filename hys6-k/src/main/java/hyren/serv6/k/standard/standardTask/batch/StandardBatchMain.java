package hyren.serv6.k.standard.standardTask.batch;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.k.standard.standardTask.StandardTaskService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class StandardBatchMain {

    private static StandardTaskService standardTaskService;

    public static void init() {
        standardTaskService = new StandardTaskService();
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(StandardBatchMain.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.run(args);
        init();
        String taskId = args[0];
        DatabaseWrapper db = new DatabaseWrapper();
        standardTaskService.standardBatch(Long.parseLong(taskId), db);
    }
}
