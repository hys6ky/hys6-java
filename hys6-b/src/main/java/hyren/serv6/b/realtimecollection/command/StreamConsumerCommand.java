package hyren.serv6.b.realtimecollection.command;

import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
@Slf4j
public class StreamConsumerCommand {

    public static void main(String[] args) {
        new SpringApplicationBuilder(ConsumerSelector.class).web(WebApplicationType.NONE).run(args);
        if (args.length != 1) {
            log.info("请按照规定的格式传入参数，必须参数不能为空");
            log.info("必须参数：sdm_consume_conf.sdm_consum_id");
            System.exit(-1);
        }
        ConsumerSelector.main(args);
    }
}
