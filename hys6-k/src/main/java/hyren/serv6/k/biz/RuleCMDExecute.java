package hyren.serv6.k.biz;

import fd.ng.core.utils.JsonUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.DqcExecMode;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.k.dm.ruleconfig.bean.SysVarCheckBean;
import hyren.serv6.k.dm.ruleconfig.commons.DqcExecution;
import hyren.serv6.k.entity.DqDefinition;
import hyren.serv6.k.utils.SpringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import java.util.Set;

@Slf4j
@SpringBootApplication
public class RuleCMDExecute {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(RuleCMDExecute.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        ApplicationContext context = application.run(args);
        SpringUtils.setAppContext(context);
        if (args == null) {
            log.error("必要参数不能为空!");
            log.error("必须参数：参数1：规则ID；参数2：验证日期");
            System.exit(-1);
        }
        if (args.length < 2) {
            log.error("必要参数不合法!");
            log.error("必须参数：参数1：规则ID；参数2：验证日期");
            System.exit(-1);
        }
        String reg_num = args[0];
        String verify_date = args[1];
        log.info("规则编号: " + reg_num + ",执行时间: " + verify_date);
        long result_num = run(reg_num, verify_date);
        log.info("规则结果编号: " + result_num);
    }

    public static long run(String reg_num, String verify_date) {
        DqDefinition dq_definition = null;
        Set<SysVarCheckBean> beans = null;
        log.info("获取DB中");
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            log.info("获取DB");
            DqDefinition dqd = new DqDefinition();
            dqd.setReg_num(reg_num);
            dq_definition = SqlOperator.queryOneObject(db, DqDefinition.class, "SELECT * FROM " + DqDefinition.TableName + " " + "WHERE reg_num=?", dqd.getReg_num()).orElseThrow(() -> (new BusinessException("获取配置信息的SQL失败!")));
            beans = DqcExecution.getSysVarCheckBean(db, dq_definition);
            if (!beans.isEmpty()) {
                log.info("规则依赖参数信息:" + JsonUtil.toJson(beans));
            }
        } catch (NullPointerException e) {
            if (null == dq_definition) {
                log.error("根据规则编号: " + reg_num + " 没有找到对应规则!");
                System.exit(2);
            }
        }
        return DqcExecution.executionRule(dq_definition, verify_date, beans, DqcExecMode.ZiDong.getCode());
    }
}
