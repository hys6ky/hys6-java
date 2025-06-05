package hyren.serv6.k.dm.ruleconfig.commons;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.*;
import hyren.serv6.commons.collection.LoadingData;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LoadingDataBean;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.k.dm.ruleconfig.bean.SysVarCheckBean;
import hyren.serv6.k.entity.DqDefinition;
import hyren.serv6.k.entity.DqResult;
import hyren.serv6.k.entity.DqSysCfg;
import hyren.serv6.k.utils.CheckBeanUtil;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/4/10 0010 上午 10:41")
@Slf4j
public class DqcExecution {

    private static final Logger logger = LogManager.getLogger();

    @Method(desc = "", logicStep = "")
    @Param(name = "dq_definition", desc = "", range = "", isBean = true)
    @Param(name = "verify_date", desc = "", range = "")
    @Param(name = "beans", desc = "", range = "")
    @Param(name = "exec_method", desc = "", range = "")
    @Return(desc = "", range = "")
    public static long executionRule(DqDefinition dq_definition, String verify_date, Set<SysVarCheckBean> beans, String exec_method) {
        if (StringUtil.isBlank(dq_definition.getReg_num().toString())) {
            throw new BusinessException("执行规则时,规则编号为空!");
        }
        if (StringUtil.isBlank(exec_method)) {
            throw new BusinessException("执行规则时,执行方式为空!");
        }
        String dq_rule_level = EdRuleLevel.JingGao.getCode();
        boolean has_exception = Boolean.FALSE;
        DqResult dq_result = new DqResult();
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            try {
                dq_rule_level = dq_definition.getFlags();
                BeanUtil.copyProperties(dq_definition, dq_result);
                dq_result.setTask_id(PrimaryKeyUtils.nextId());
                dq_result.setVerify_date(verify_date);
                dq_result.setStart_date(DateUtil.getSysDate());
                dq_result.setStart_time(DateUtil.getSysTime());
                dq_result.setExec_mode(exec_method);
                dq_result.setVerify_result(DqcVerifyResult.ZhengChang.getCode());
                dq_result.setVerify_sql(dq_definition.getSpecify_sql());
                dq_result.setErr_dtl_sql(dq_definition.getErr_data_sql());
                dq_result.setDl_stat(DqcDlStat.ZhengChang.getCode());
                long execution_start_time = System.currentTimeMillis();
                String[] verify_sql = dq_result.getVerify_sql().split(";");
                String index1_sql;
                String index2_sql = "";
                if (verify_sql.length == 1) {
                    index1_sql = verify_sql[0].trim();
                } else if (verify_sql.length == 2) {
                    index1_sql = verify_sql[0].trim();
                    index2_sql = verify_sql[1].trim();
                } else {
                    throw new BusinessException("请填写检查sql!");
                }
                for (SysVarCheckBean bean : beans) {
                    index1_sql = index1_sql.replace(bean.getName(), bean.getValue());
                    index2_sql = index2_sql.replace(bean.getName(), bean.getValue());
                }
                Map<String, Object> map_result_1 = new HashMap<>();
                if (StringUtil.isNotBlank(index1_sql)) {
                    try {
                        new ProcessingData() {

                            @Override
                            public void dealLine(Map<String, Object> map) {
                                map_result_1.putAll(map);
                            }
                        }.getDataLayer(index1_sql, db);
                    } catch (Exception e) {
                        logger.warn("执行获取指标1结果的sql时出错!" + e.getMessage());
                        e.printStackTrace();
                        dq_result.setVerify_result(DqcVerifyResult.ZhiXingShiBai.getCode());
                    }
                    if (IsFlag.Shi.getCode().equals(dq_definition.getIs_saveindex1())) {
                        dq_result.setCheck_index1(Integer.valueOf(map_result_1.get("index1").toString()));
                    }
                    if ("TAB NAN".equals(dq_definition.getCase_type())) {
                        if (dq_result.getCheck_index1().longValue() > 0) {
                            dq_result.setVerify_result(DqcVerifyResult.ZhengChang.getCode());
                        } else {
                            dq_result.setVerify_result(DqcVerifyResult.YiChang.getCode());
                        }
                    } else {
                        if (dq_result.getCheck_index1().longValue() > 0) {
                            dq_result.setVerify_result(DqcVerifyResult.YiChang.getCode());
                        }
                    }
                }
                Map<String, Object> map_result_2 = new HashMap<>();
                if (StringUtil.isNotBlank(index2_sql)) {
                    try {
                        new ProcessingData() {

                            @Override
                            public void dealLine(Map<String, Object> map) {
                                map_result_2.putAll(map);
                            }
                        }.getDataLayer(index2_sql, db);
                    } catch (Exception e) {
                        logger.warn("执行获取指标1结果的sql时出错!" + e.getMessage());
                        e.printStackTrace();
                        dq_result.setVerify_result(DqcVerifyResult.ZhiXingShiBai.getCode());
                    }
                    if (IsFlag.Shi.getCode().equals(dq_definition.getIs_saveindex2())) {
                        dq_result.setCheck_index2(Integer.valueOf(map_result_2.get("index2").toString()));
                    }
                }
                dq_result.setEnd_date(DateUtil.getSysDate());
                dq_result.setEnd_time(DateUtil.getSysTime());
                long execution_end_time = System.currentTimeMillis();
                dq_result.setElapsed_ms((int) (execution_end_time - execution_start_time));
                if (IsFlag.Shi.getCode().equals(dq_result.getIs_saveindex3()) && StringUtil.isNotBlank(dq_result.getErr_dtl_sql())) {
                    recordIndicator3Data(db, dq_result, beans);
                }
            } catch (Exception e) {
                has_exception = Boolean.TRUE;
                logger.info("规则级别为严重，且发生了异常：" + e);
                dq_result.setDl_stat(DqcDlStat.DengDaiChuLi.getCode());
                dq_result.setErrno(e.getMessage());
                if (EdRuleLevel.YanZhong.getCode().equals(dq_rule_level)) {
                    dq_result.setVerify_result(DqcVerifyResult.ZhiXingShiBai.getCode());
                }
            } finally {
                dq_result.add(db);
                db.commit();
            }
            if (EdRuleLevel.YanZhong.getCode().equals(dq_rule_level) && has_exception) {
                if (DqcExecMode.ShouGong.getCode().equals(exec_method)) {
                    throw new BusinessException("手动执行,规则级别为严重，且发生了异常");
                } else {
                    logger.info("作业调度执行,规则级别为严重，且发生了异常，退出,状态码为: -1");
                    System.exit(-1);
                }
            }
        }
        return dq_result.getTask_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "dq_definition", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Set<SysVarCheckBean> getSysVarCheckBean(DatabaseWrapper db, DqDefinition dqd) {
        List<DqSysCfg> dq_sys_cfg_s = SqlOperator.queryList(db, DqSysCfg.class, "select * from " + DqSysCfg.TableName);
        Map<String, String> dq_sys_cfg_map = new HashMap<>();
        dq_sys_cfg_s.forEach(dq_sys_cfg -> dq_sys_cfg_map.put(dq_sys_cfg.getVar_name(), dq_sys_cfg.getVar_value()));
        dq_sys_cfg_map.put("#{TX_DATE}", DateUtil.getSysDate());
        dq_sys_cfg_map.put("#{TX_DATE10}", DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()).toString());
        Set<SysVarCheckBean> sysVarCheckBeans = new HashSet<>();
        List<String> str_s = CheckBeanUtil.getBeanValueWithPattern(dqd, "(?=#\\{)(.*?)(?>\\})");
        str_s.forEach(str -> {
            SysVarCheckBean scb = new SysVarCheckBean();
            if (dq_sys_cfg_map.containsKey(str)) {
                scb.setName(str);
                scb.setIsEff(IsFlag.Shi.getValue());
                scb.setValue(dq_sys_cfg_map.get(str));
            } else {
                scb.setName(str);
                scb.setIsEff(IsFlag.Fou.getValue());
                scb.setValue(str);
            }
            sysVarCheckBeans.add(scb);
        });
        return sysVarCheckBeans;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "dq_definition", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getEffDepJobs(DatabaseWrapper db, DqDefinition dq_definition) {
        if (StringUtil.isBlank(dq_definition.getReg_num().toString())) {
            throw new BusinessException("规则编号为空!");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT etl_sys_id,etl_job,job_eff_flag,job_disp_status FROM " + EtlJobDef.TableName + " WHERE" + " job_eff_flag = ?").addParam(Job_Effective_Flag.YES.getCode());
        asmSql.addLikeParam("etl_job", '%' + dq_definition.getReg_num().toString() + '%');
        asmSql.addSql("UNION");
        asmSql.addSql("SELECT etl_sys_id,etl_job,job_eff_flag,job_disp_status FROM " + EtlJobCur.TableName + " WHERE" + " job_eff_flag = ?").addParam(Job_Effective_Flag.YES.getCode());
        asmSql.addLikeParam("etl_job", '%' + dq_definition.getReg_num().toString() + '%');
        asmSql.addSql("UNION");
        asmSql.addSql("SELECT etl_sys_id,etl_job,job_eff_flag,job_disp_status FROM " + EtlJobDispHis.TableName + " WHERE job_eff_flag = ?").addParam(Job_Effective_Flag.YES.getCode());
        asmSql.addLikeParam("etl_job", '%' + dq_definition.getReg_num().toString() + '%');
        return SqlOperator.queryList(db, asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dq_definition", desc = "", range = "")
    @Param(name = "beans", desc = "", range = "")
    @Return(desc = "", range = "")
    private static void recordIndicator3Data(DatabaseWrapper db, DqResult dq_result, Set<SysVarCheckBean> beans) {
        if (StringUtil.isBlank(dq_result.getTask_id().toString())) {
            throw new BusinessException("在记录指标3的数据时，传入的任务编号为空!");
        }
        String sql = dq_result.getErr_dtl_sql();
        for (SysVarCheckBean bean : beans) {
            sql = sql.replace(bean.getName(), bean.getValue());
        }
        try {
            String dqc_table_name = Constant.DQC_TABLE + dq_result.getTask_id();
            LoadingDataBean ldbbean = new LoadingDataBean();
            ldbbean.setTableName(dqc_table_name);
            long dsl_id = new LoadingData(ldbbean).intoDataLayer(sql, db);
            DqIndex3record dq_index3record = new DqIndex3record();
            dq_index3record.setRecord_id(PrimaryKeyUtils.nextId());
            dq_index3record.setTable_name(dqc_table_name);
            dq_index3record.setTable_col(dq_result.getTarget_key_fields());
            dq_index3record.setTable_size(new BigDecimal("0"));
            dq_index3record.setDqc_ts("");
            dq_index3record.setFile_type("");
            dq_index3record.setFile_path("");
            dq_index3record.setRecord_date(DateUtil.getSysDate());
            dq_index3record.setRecord_time(DateUtil.getSysTime());
            dq_index3record.setTask_id(dq_result.getTask_id());
            dq_index3record.setDsl_id(dsl_id);
            dq_index3record.add(db);
        } catch (Exception e) {
            dq_result.setVerify_result(DqcVerifyResult.ZhiXingShiBai.getCode());
            logger.info("在插入指标3的全量数据记录信息时失败，任务编号为：" + dq_result.getTask_id());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "beans", desc = "", range = "")
    @Param(name = "sql", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String executionSqlCheck(Set<SysVarCheckBean> beans, String... sql_s) {
        if (StringUtil.isBlank(Arrays.toString(sql_s))) {
            throw new BusinessException("需要执行的sql为空!");
        }
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            for (String sql : sql_s) {
                sql = sql.replace("\n", "");
                sql = sql.replace("\t", "");
                for (SysVarCheckBean bean : beans) {
                    sql = sql.replace(bean.getName(), bean.getValue());
                }
                if (StringUtil.isBlank(sql)) {
                    throw new BusinessException("执行sql的时候,sql为空!");
                }
                try {
                    new ProcessingData() {

                        @Override
                        public void dealLine(Map<String, Object> map) {
                        }
                    }.getDataLayer(sql, db);
                } catch (Exception e) {
                    throw new BusinessException(e.getMessage());
                }
            }
        }
        return "success";
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "job_method", desc = "", range = "")
    @Return(desc = "", range = "")
    @Deprecated
    public static String getRuleTaskId(String job_method) {
        Calendar cal = Calendar.getInstance();
        Date date = cal.getTime();
        String time = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(date);
        int random = (int) (Math.random() * 900) + 100;
        return job_method + time + random;
    }
}
