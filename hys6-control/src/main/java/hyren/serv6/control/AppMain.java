package hyren.serv6.control;

import fd.ng.core.cmd.ArgsParser;
import fd.ng.core.utils.DateUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Job_Status;
import hyren.serv6.base.entity.EtlSys;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.jobUtil.task.TaskSqlHelper;
import hyren.serv6.control.server.ControlManageServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import java.time.LocalDate;

@Slf4j
@SpringBootApplication
public class AppMain {

    public static void main(String[] args) {
        new SpringApplicationBuilder(AppMain.class).web(WebApplicationType.NONE).run(args);
        ArgsParser CMD_ARGS = new ArgsParser().defOptionPair("etl.date", true, "跑批日期yyyyMMDD").defOptionPair("sys.code", true, "调度系统代码").defOptionPair("-AS", true, "是否自动日切").defOptionPair("-CR", true, "是否为续跑").defOptionPair("end.date", true, "跑批结束日期yyyyMMDD").parse(args);
        String bathDateStr = CMD_ARGS.opt("etl.date").value;
        boolean isResumeRun = CMD_ARGS.opt("-CR").value.equals(IsFlag.Shi.getCode());
        boolean isAutoShift = CMD_ARGS.opt("-AS").value.equals(IsFlag.Shi.getCode());
        String strSystemCode = CMD_ARGS.opt("sys.code").value;
        String endDate = CMD_ARGS.opt("end.date").value;
        try {
            EtlSys etlSys = TaskSqlHelper.getEltSysBySysCode(strSystemCode);
            LocalDate bathDate = LocalDate.parse(bathDateStr, DateUtil.DATE_DEFAULT);
            if (!Job_Status.STOP.getCode().equals(etlSys.getSys_run_status())) {
                throw new AppSystemException("调度系统不在停止状态：" + strSystemCode);
            } else if (isResumeRun) {
                LocalDate currBathDate = DateUtil.parseStr2DateWith8Char(etlSys.getCurr_bath_date());
                if (!currBathDate.equals(bathDate)) {
                    throw new AppSystemException("续跑日期与当前批量日期不一致：" + strSystemCode);
                }
            }
            log.info(String.format("开始启动Agent服务，跑批日期：%s，结束日期：%s,系统代码：%s，" + "是否续跑：%s，是否自动日切：%s", bathDate, endDate, strSystemCode, isResumeRun, isAutoShift));
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                throw new AppSystemException(t.getName() + " 线程产生了异常: " + e);
            });
            ControlManageServer cm = new ControlManageServer(strSystemCode, bathDateStr, endDate, isResumeRun, isAutoShift);
            cm.initCMServer();
            cm.runCMServer();
            log.info("-------------- Control Agent服务启动完成 --------------");
        } catch (Exception ex) {
            log.error(" 最后  Exception happened!", ex);
        }
    }
}
