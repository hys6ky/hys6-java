package hyren.serv6.b.datareceive.run;

import hyren.serv6.b.datareceive.DataReceiveService;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@Slf4j
@SpringBootApplication
public class ReceiveJobCommand {

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            log.info("请按照规定的格式传入参数,且必须参数不能为空");
            log.info("必须参数：参数1：数据接收任务id；批日期；");
            log.info("非必须参数：参数3-N：url占位符参数 如:condition=参数2：跑value condition1=value1");
            System.exit(-1);
        }
        new SpringApplicationBuilder(ReceiveJobCommand.class).web(WebApplicationType.NONE).run(args);
        log.info("参数1主键数据接收任务id为:{}", args[0]);
        log.info("参数2跑批日期为:{}", args[1]);
        StringBuilder sqlParam = new StringBuilder();
        if (args.length > 2) {
            for (int i = 2; i < args.length; i++) {
                sqlParam.append(args[i]).append(Constant.SQLDELIMITER);
            }
            sqlParam.delete(sqlParam.length() - Constant.SQLDELIMITER.length(), sqlParam.length());
        }
        log.info("sql占位符：{}", sqlParam);
        try {
            DataReceiveService service = new DataReceiveService();
            service.unloadAnalData(Long.parseLong(args[0]), args[1], sqlParam.toString());
            log.info("数据接收卸数成功");
            System.exit(0);
        } catch (Exception e) {
            log.error("数据接收失败：" + e);
            System.exit(-1);
        }
    }
}
