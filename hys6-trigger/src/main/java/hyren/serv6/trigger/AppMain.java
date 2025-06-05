package hyren.serv6.trigger;

import fd.ng.core.cmd.ArgsParser;
import hyren.serv6.base.codes.Job_Status;
import hyren.serv6.base.entity.EtlSys;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import hyren.serv6.trigger.server.TriggerManageServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@Slf4j
@SpringBootApplication
public class AppMain {

    public static void main(String[] args) {
        try {
            new SpringApplicationBuilder(AppMain.class).web(WebApplicationType.NONE).run(args);
            ArgsParser CMD_ARGS = new ArgsParser().defOptionPair("sys.code", true, "调度系统代码").parse(args);
            String strSystemCode = CMD_ARGS.opt("sys.code").value;
            System.out.println("开始启动Trigger程序，调度系统编号为：" + strSystemCode);
            EtlSys etlSys = TaskSqlHelper.getEltSysBySysCode(strSystemCode);
            if (Job_Status.ofEnumByCode(etlSys.getSys_run_status()) != Job_Status.RUNNING) {
                System.out.println("调度系统不为运行状态，Trigger程序启动失败。");
                return;
            }
            TriggerManageServer triggerManageServer = new TriggerManageServer(strSystemCode);
            triggerManageServer.runCMServer();
            log.info("-------------- Trigger Agent服务启动完成 --------------");
        } catch (Exception ex) {
            throw new AppSystemException("Exception happened!", ex);
        }
    }
}
