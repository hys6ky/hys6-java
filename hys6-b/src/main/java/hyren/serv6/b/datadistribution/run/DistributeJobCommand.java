package hyren.serv6.b.datadistribution.run;

import hyren.serv6.b.receive.distribute.DistributeService;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@Slf4j
@SpringBootApplication
public class DistributeJobCommand {

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            log.info("请按照规定的格式传入参数,且必须参数不能为空");
            log.info("必须参数：参数1：主键分发id；参数2：跑批日期；");
            log.info("非必须参数：参数3-N：sql占位符参数 如:condition=value condition1=value1");
            System.exit(-1);
        }
        new SpringApplicationBuilder(DistributeJobCommand.class).web(WebApplicationType.NONE).run(args);
        log.info("参数1主键分发id为:{}", args[0]);
        log.info("参数2跑批日期为:{}", args[1]);
        StringBuilder sqlParam = new StringBuilder();
        if (args.length > 2) {
            for (int i = 2; i < args.length; i++) {
                sqlParam.append(args[i]).append(Constant.SQLDELIMITER);
            }
            sqlParam.delete(sqlParam.length() - Constant.SQLDELIMITER.length(), sqlParam.length());
        }
        log.info("sql占位符：{}", sqlParam);
        DistributeService distributeService = new DistributeService();
        try {
            distributeService.unloadDistributeData(Long.parseLong(args[0]), args[1], sqlParam.toString());
            log.info("数据分发卸数成功");
            System.exit(0);
        } catch (Exception e) {
            log.error("数据分发失败：" + e);
            System.exit(-1);
        }
    }
}
