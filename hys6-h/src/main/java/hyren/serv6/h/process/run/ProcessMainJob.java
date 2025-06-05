package hyren.serv6.h.process.run;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.loader.*;
import hyren.serv6.h.process.loader.business.impl.ProcessBusinessImpl;
import hyren.serv6.h.process.loader.chooser.LoaderChooserImpl;
import hyren.serv6.h.process.loader.context.ContextImpl;
import hyren.serv6.h.process.loader.executor.Executor;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;
import hyren.serv6.h.process.utils.ProcessTableConfBeanUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import java.io.IOException;
import java.util.List;

@Slf4j
@SpringBootApplication
public class ProcessMainJob {

    public static void main(String[] args) throws Exception {
        new SpringApplicationBuilder(ProcessMainJob.class).web(WebApplicationType.NONE).run(args);
        if (args == null || args.length < 4) {
            log.info("请按照规定的格式传入参数,必须参数不能为空");
            log.info("必须参数: 参数1: 加工模型表id;参数2: 加工作业表id;参数3: 调度日期;参数4: 作业名;");
            log.info("非必须参数: 参数5-N: sql占位符参数 condition=value condition1=value1");
            System.exit(-1);
        }
        if (args.length == 4) {
            run(args[0], args[1], args[2], args[3]);
        } else {
            StringBuilder sqlParam = new StringBuilder();
            for (int i = 4; i < args.length; i++) {
                log.debug("SQL占位符参数: {}", args[i]);
                List<String> param_split = StringUtil.split(args[i], "=");
                String key = param_split.get(0);
                String value;
                if (key.equalsIgnoreCase("txdate")) {
                    value = DateUtil.parseStr2DateWith8Char(param_split.get(1)).toString();
                } else {
                    value = param_split.get(1);
                }
                sqlParam.append(key).append("=").append(value).append(Constant.SQLDELIMITER);
            }
            sqlParam.delete(sqlParam.length() - Constant.SQLDELIMITER.length(), sqlParam.length());
            run(args[0], args[1], args[2], args[3], sqlParam.toString());
        }
    }

    public static void run(String moduleTableId, String jobTableId, String etlDate, String jobName) throws Exception {
        run(moduleTableId, jobTableId, etlDate, jobName, "");
    }

    public static void run(String moduleTableId, String jobTableId, String etlDate, String jobName, String sqlParams) throws Exception {
        log.info("--参数 moduleTableId : {}", moduleTableId);
        log.info("--参数 jobTableId    : {}", jobTableId);
        log.info("--参数 etlDate       : {}", etlDate);
        log.info("--参数 jobName       : {}", jobName);
        log.info("--参数 sqlParams     : {}", sqlParams);
        ProcessJobTableConfBean processJobTableConfBean = ProcessTableConfBeanUtil.getProcessTableConfBean(moduleTableId, jobTableId, etlDate, jobName, sqlParams);
        ProcessTableConfBeanUtil.generateJobSerializeFile(processJobTableConfBean);
        IContext jobContext = new ContextImpl(processJobTableConfBean);
        ILoaderChooser loaderSwitch = new LoaderChooserImpl();
        try {
            ILoader loader = loaderSwitch.choiceLoader(processJobTableConfBean);
            IBusiness iBusiness = new ProcessBusinessImpl(loader);
            IExecutor loaderExecutor = new Executor();
            ProcessJobRunStatusEnum processJobRunStatusEnum = loaderExecutor.registerJobContext(jobContext).registerBusiness(iBusiness).registerLoader(loader).execute();
            if (processJobRunStatusEnum == ProcessJobRunStatusEnum.FINISHED) {
                System.exit(0);
            } else {
                System.exit(processJobRunStatusEnum.getCode());
            }
        } catch (Exception e) {
            throw new AppSystemException("e: " + e);
        }
    }
}
