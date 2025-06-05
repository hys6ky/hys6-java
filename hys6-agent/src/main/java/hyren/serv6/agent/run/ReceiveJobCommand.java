package hyren.serv6.agent.run;

import hyren.daos.base.utils.ActionResult;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
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
            String url = AgentActionUtil.getServerUrl(AgentActionUtil.UNLOADRECEIVEDATA);
            log.info("url数据接收卸数url:{}", url);
            HttpClient.ResponseValue resVal = new HttpClient().addData("dr_task_id", Long.parseLong(args[0])).addData("curr_bath_date", args[1]).addData("drParams", sqlParam.toString()).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new AppSystemException(String.format("数据接收卸数失败:%s", ar.getMessage()));
            }
            log.info("数据接收卸数成功");
            System.exit(0);
        } catch (Exception e) {
            log.error("数据接收失败：" + e);
            System.exit(-1);
        }
    }
}
