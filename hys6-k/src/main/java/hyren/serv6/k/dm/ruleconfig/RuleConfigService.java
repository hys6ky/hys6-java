package hyren.serv6.k.dm.ruleconfig;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.k.dm.ruleconfig.bean.RuleConfSearchBean;
import hyren.serv6.k.dm.ruleconfig.bean.SysVarCheckBean;
import hyren.serv6.k.dm.ruleconfig.commons.DqcExecution;
import hyren.serv6.k.entity.DqDefinition;
import hyren.serv6.k.entity.DqHelpInfo;
import hyren.serv6.k.entity.DqResult;
import hyren.serv6.k.entity.DqRuleDef;
import hyren.serv6.k.utils.CheckBeanUtil;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class RuleConfigService {

    private static final Logger logger = LogManager.getLogger();

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Node> getRuleConfigTreeData() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.FALSE);
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(TreePageSource.DATA_MANAGEMENT, UserUtil.getUser(), treeConf);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dq_definition", desc = "", range = "", isBean = true)
    public void addDqDefinition(DqDefinition dq_definition) {
        if (StringUtil.isBlank(dq_definition.getCase_type())) {
            throw new BusinessException("规则类型为空!");
        }
        dq_definition.setReg_num(PrimaryKeyUtils.nextId());
        dq_definition.setApp_updt_dt(DateUtil.getSysDate());
        dq_definition.setApp_updt_ti(DateUtil.getSysTime());
        if (StringUtil.isBlank(dq_definition.getIs_saveindex1())) {
            dq_definition.setIs_saveindex1(IsFlag.Fou.getCode());
        }
        if (StringUtil.isBlank(dq_definition.getIs_saveindex2())) {
            dq_definition.setIs_saveindex2(IsFlag.Fou.getCode());
        }
        if (StringUtil.isBlank(dq_definition.getIs_saveindex3())) {
            dq_definition.setIs_saveindex3(IsFlag.Fou.getCode());
        }
        dq_definition.setUser_id(UserUtil.getUserId());
        if (StringUtil.isNotBlank(dq_definition.getSpecify_sql())) {
            dq_definition.setSpecify_sql(dq_definition.getSpecify_sql().replace("\n", " "));
            dq_definition.setSpecify_sql(dq_definition.getSpecify_sql().replace("\t", " "));
        }
        if (StringUtil.isNotBlank(dq_definition.getErr_data_sql())) {
            dq_definition.setErr_data_sql(dq_definition.getErr_data_sql().replace("\n", " "));
            dq_definition.setErr_data_sql(dq_definition.getErr_data_sql().replace("\t", " "));
        }
        dq_definition.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "reg_num", desc = "", range = "")
    public void deleteDqDefinition(long reg_num) {
        if (checkRegNumIsExist(reg_num)) {
            DboExecute.deletesOrThrow("通过编号删除数据失败!", "delete from " + DqDefinition.TableName + " where user_id=? and reg_num=?", UserUtil.getUserId(), reg_num);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "reg_num", desc = "", range = "")
    public void releaseDeleteDqDefinition(Long[] reg_num) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("delete from " + DqDefinition.TableName + " where user_id=?");
        asmSql.addParam(UserUtil.getUserId());
        asmSql.addORParam("reg_num ", reg_num);
        Dbo.execute(asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dq_definition", desc = "", range = "")
    public void updateDqDefinition(DqDefinition dq_definition) {
        Validator.notBlank(dq_definition.getReg_num().toString(), "修改规则编号为空");
        if (!checkRegNumIsExist(dq_definition.getReg_num())) {
            throw new BusinessException("修改的规则已经不存在!");
        }
        dq_definition.setApp_updt_dt(DateUtil.getSysDate());
        dq_definition.setApp_updt_ti(DateUtil.getSysTime());
        if (StringUtil.isBlank(dq_definition.getIs_saveindex1())) {
            dq_definition.setIs_saveindex1(IsFlag.Fou.getCode());
        }
        if (StringUtil.isBlank(dq_definition.getIs_saveindex2())) {
            dq_definition.setIs_saveindex2(IsFlag.Fou.getCode());
        }
        if (StringUtil.isBlank(dq_definition.getIs_saveindex3())) {
            dq_definition.setIs_saveindex3(IsFlag.Fou.getCode());
        }
        dq_definition.setUser_id(UserUtil.getUserId());
        if (StringUtil.isNotBlank(dq_definition.getSpecify_sql())) {
            dq_definition.setSpecify_sql(dq_definition.getSpecify_sql().replace("\n", " "));
            dq_definition.setSpecify_sql(dq_definition.getSpecify_sql().replace("\t", " "));
        }
        if (StringUtil.isNotBlank(dq_definition.getErr_data_sql())) {
            dq_definition.setErr_data_sql(dq_definition.getErr_data_sql().replace("\n", " "));
            dq_definition.setErr_data_sql(dq_definition.getErr_data_sql().replace("\t", " "));
        }
        dq_definition.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDqDefinitionInfos(Integer currPage, Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select dql.*,? as job_status from " + DqDefinition.TableName + " dql where user_id=?").addParam(Job_Effective_Flag.NO.getCode()).addParam(UserUtil.getUserId());
        asmSql.addSql(" order by app_updt_dt desc,app_updt_ti desc");
        List<Map<String, Object>> dqd_list = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        DqDefinition dq_definition = new DqDefinition();
        for (Map<String, Object> dqd : dqd_list) {
            dq_definition.setReg_num(Long.valueOf(dqd.get("reg_num").toString()));
            List<Map<String, Object>> effDepJobs = DqcExecution.getEffDepJobs(Dbo.db(), dq_definition);
            if (!effDepJobs.isEmpty()) {
                dqd.put("job_status", Job_Effective_Flag.YES.getCode());
            }
        }
        Map<String, Object> dqd_map = new HashMap<>();
        dqd_map.put("rule_dqd_data_s", dqd_list);
        dqd_map.put("totalSize", page.getTotalSize());
        return dqd_map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "reg_num", desc = "", range = "")
    @Return(desc = "", range = "")
    public DqDefinition getDqDefinition(long reg_num) {
        if (!checkRegNumIsExist(reg_num)) {
            throw new BusinessException("查询的规则信息已经不存在!");
        }
        DqDefinition dq_definition = Dbo.queryOneObject(DqDefinition.class, "select * from " + DqDefinition.TableName + " where reg_num=?", reg_num).orElseThrow(() -> (new BusinessException("获取规则信息的SQL错误!")));
        if (!StringUtil.isEmpty(dq_definition.getErr_data_sql())) {
            dq_definition.setErr_data_sql(AesUtil.encrypt(dq_definition.getErr_data_sql()));
        }
        if (!StringUtil.isEmpty(dq_definition.getSpecify_sql())) {
            dq_definition.setSpecify_sql(AesUtil.encrypt(dq_definition.getSpecify_sql()));
        }
        return dq_definition;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getColumnsByTableName(String table_name) {
        Validator.notBlank(table_name, "查询表名不能为空!");
        return DataTableUtil.getColumnByTableName(Dbo.db(), table_name);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getTableOneDSLInfo(String table_name) {
        return AesUtil.encrypt(JsonUtil.toJson(ProcessingData.getLayerByTable(table_name, Dbo.db()).get(0)));
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<DqRuleDef> getDqRuleDef() {
        return Dbo.queryList(DqRuleDef.class, "select * from " + DqRuleDef.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<DqHelpInfo> getDqHelpInfo() {
        return Dbo.queryList(DqHelpInfo.class, "select * from " + DqHelpInfo.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "pro_id", desc = "", range = "")
    @Param(name = "task_id", desc = "", range = "")
    @Param(name = "reg_num", desc = "", range = "")
    @Return(desc = "", range = "")
    public void saveETLJob(Long pro_id, Long task_id, String reg_num) {
        DqDefinition dqDefinition = Dbo.queryOneObject(DqDefinition.class, "SELECT * from " + DqDefinition.TableName + " where reg_num = ? limit  1 ", Long.valueOf(reg_num)).orElseThrow(() -> new BusinessException("未查询到规则信息!"));
        if (StringUtil.isEmpty(dqDefinition.getSpecify_sql()) || StringUtil.isEmpty(dqDefinition.getErr_data_sql())) {
            throw new BusinessException("请检查规则中的SQL信息，未获取到此规则下的SQL信息！");
        }
        int i = EtlJobUtil.saveJob(reg_num, DataSourceType.DQC, pro_id, task_id, null);
        if (i != 0) {
            throw new BusinessException("保存到作业调度失败!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "pro_id", desc = "", range = "")
    @Param(name = "task_id", desc = "", range = "")
    @Param(name = "reg_nums", desc = "", range = "")
    @Return(desc = "", range = "")
    public StringBuilder batchETLJob(Long pro_id, Long task_id, String reg_nums) {
        String[] split = reg_nums.split(",");
        StringBuilder errDataMessage = new StringBuilder();
        for (String reg_num : split) {
            DqDefinition dqDefinition = Dbo.queryOneObject(DqDefinition.class, "SELECT * from " + DqDefinition.TableName + " where reg_num = ? limit  1 ", Long.valueOf(reg_num)).orElse(null);
            if (dqDefinition == null) {
                errDataMessage.append("未找到规则编号为 【" + reg_num + "】的数据!  ");
                continue;
            }
            if (StringUtil.isEmpty(dqDefinition.getSpecify_sql()) || StringUtil.isEmpty(dqDefinition.getErr_data_sql())) {
                errDataMessage.append(dqDefinition.getReg_name() + " 请检查规则中的SQL信息，未获取到此规则下的SQL信息!  ");
                continue;
            }
            int i = EtlJobUtil.saveJob(reg_num, DataSourceType.DQC, pro_id, task_id, null);
            if (i != 0) {
                errDataMessage.append(dqDefinition.getReg_name() + " 保存到作业调度失败!  ");
                continue;
            }
        }
        return errDataMessage;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ruleConfSearchBean", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public Map<String, Object> searchDqDefinitionInfos(RuleConfSearchBean ruleConfSearchBean, Integer currPage, Integer pageSize) {
        if (CheckBeanUtil.checkFullNull(ruleConfSearchBean)) {
            return getDqDefinitionInfos(currPage, pageSize);
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT * FROM " + DqDefinition.TableName + " where user_id = ?").addParam(UserUtil.getUserId());
        if (StringUtil.isNotBlank(ruleConfSearchBean.getReg_num())) {
            asmSql.addLikeParam("cast(reg_num as varchar)", '%' + ruleConfSearchBean.getReg_num() + '%');
        }
        if (StringUtil.isNotBlank(ruleConfSearchBean.getTarget_tab())) {
            asmSql.addLikeParam("target_tab", '%' + ruleConfSearchBean.getTarget_tab() + '%');
        }
        if (StringUtil.isNotBlank(ruleConfSearchBean.getRule_tag())) {
            asmSql.addLikeParam("rule_tag", '%' + ruleConfSearchBean.getRule_tag() + '%');
        }
        if (StringUtil.isNotBlank(ruleConfSearchBean.getReg_name())) {
            asmSql.addLikeParam("reg_name", '%' + ruleConfSearchBean.getReg_name() + '%');
        }
        if (StringUtil.isNotBlank(ruleConfSearchBean.getRule_src())) {
            asmSql.addLikeParam("rule_src", '%' + ruleConfSearchBean.getRule_src() + '%');
        }
        if (null != ruleConfSearchBean.getCase_type() && ruleConfSearchBean.getCase_type().length > 0) {
            asmSql.addORParam("case_type", ruleConfSearchBean.getCase_type());
        }
        asmSql.addSql(" order by app_updt_dt desc,app_updt_ti desc");
        List<Map<String, Object>> dqd_list = Dbo.queryList(asmSql.sql(), asmSql.params());
        DqDefinition dq_definition = new DqDefinition();
        for (Map<String, Object> dqd : dqd_list) {
            dqd.put("job_status", Job_Effective_Flag.NO.getCode());
            dq_definition.setReg_num(Long.valueOf(dqd.get("reg_num").toString()));
            List<Map<String, Object>> effDepJobs = DqcExecution.getEffDepJobs(Dbo.db(), dq_definition);
            if (effDepJobs.size() > 0) {
                dqd.put("job_status", Job_Effective_Flag.YES.getCode());
            }
        }
        List<Map<String, Object>> search_data_list = new ArrayList<>();
        dqd_list.forEach(dqd -> {
            Job_Effective_Flag job_flag = Job_Effective_Flag.ofEnumByCode(dqd.get("job_status").toString());
            if (null != ruleConfSearchBean.getJob_status() && ruleConfSearchBean.getJob_status().length > 0) {
                for (String job_status : ruleConfSearchBean.getJob_status()) {
                    if (job_flag == Job_Effective_Flag.ofEnumByCode(job_status)) {
                        search_data_list.add(dqd);
                    }
                }
            } else if (null != ruleConfSearchBean.getCase_type() && ruleConfSearchBean.getCase_type().length > 0) {
                for (String c_type : ruleConfSearchBean.getCase_type()) {
                    if (dqd.get("case_type").toString().equals(c_type)) {
                        search_data_list.add(dqd);
                    }
                }
            } else {
                search_data_list.add(dqd);
            }
        });
        List<Map<String, Object>> pageSearch = new ArrayList<>();
        int size = search_data_list.size();
        for (int i = 0; i < pageSize; i++) {
            if (size <= ((currPage - 1) * pageSize + i)) {
                break;
            }
            pageSearch.add(search_data_list.get((currPage - 1) * pageSize + i));
        }
        Map<String, Object> search_data_map = new HashMap<>();
        search_data_map.put("rule_dqd_data_s", pageSearch);
        search_data_map.put("totalSize", search_data_list.size());
        return search_data_map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "reg_num", desc = "", range = "")
    @Param(name = "verify_date", desc = "", range = "")
    public long manualExecution(long reg_num, String verify_date) {
        DqDefinition dqd = new DqDefinition();
        dqd.setReg_num(reg_num);
        DqDefinition dq_definition = Dbo.queryOneObject(DqDefinition.class, "SELECT * FROM " + DqDefinition.TableName + " " + "WHERE reg_num=?", dqd.getReg_num()).orElseThrow(() -> (new BusinessException("获取配置信息的SQL失败!")));
        if (!"TAB NAN".equals(dq_definition.getCase_type())) {
            if (StringUtil.isEmpty(dq_definition.getSpecify_sql()) || StringUtil.isEmpty(dq_definition.getErr_data_sql())) {
                throw new BusinessException("规则SQL信息为空，请检查规则！");
            }
        } else {
            if (StringUtil.isEmpty(dq_definition.getSpecify_sql())) {
                throw new BusinessException("规则SQL信息为空，请检查规则！");
            }
        }
        Set<SysVarCheckBean> beans = DqcExecution.getSysVarCheckBean(Dbo.db(), dq_definition);
        return DqcExecution.executionRule(dq_definition, verify_date, beans, DqcExecMode.ShouGong.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "task_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getCheckIndex3(long task_id) {
        List<Map<String, Object>> check_index3_list = new ArrayList<>();
        DqResult dq_result = new DqResult();
        dq_result.setTask_id(task_id);
        Validator.notBlank(dq_result.getTask_id().toString(), "获取指标3结果时,传入的任务标号为空!");
        dq_result = Dbo.queryOneObject(DqResult.class, "select * from " + DqResult.TableName + " where task_id=?", dq_result.getTask_id()).orElseThrow(() -> (new BusinessException("获取规则执行任务结果的SQL异常!")));
        IsFlag is_save_index3 = IsFlag.ofEnumByCode(dq_result.getIs_saveindex3());
        if (is_save_index3 == IsFlag.Fou) {
            return null;
        } else if (is_save_index3 == IsFlag.Shi) {
            DqcVerifyResult dqcVerifyResult = DqcVerifyResult.ofEnumByCode(dq_result.getVerify_result());
            if (dqcVerifyResult == DqcVerifyResult.ZhiXingShiBai) {
                logger.warn("执行任务id: " + dq_result.getTask_id() + " 执行结果: " + dqcVerifyResult.getValue() + ",未保存指标3结果!");
            } else if (dqcVerifyResult == DqcVerifyResult.YiChang || dqcVerifyResult == DqcVerifyResult.ZhengChang) {
                DqIndex3record dq_index3record = Dbo.queryOneObject(DqIndex3record.class, "select * from " + DqIndex3record.TableName + " where task_id=?", dq_result.getTask_id()).orElseThrow(() -> (new BusinessException("获取任务指标3存储记录的SQL异常!")));
                String sql = "select * from " + dq_index3record.getTable_name();
                try {
                    new ProcessingData() {

                        @Override
                        public void dealLine(Map<String, Object> map) {
                            check_index3_list.add(map);
                        }
                    }.getPageDataLayer(sql, Dbo.db(), 1, 10, dq_index3record.getDsl_id());
                } catch (Exception e) {
                    throw new BusinessException("获取指标3存储记录数据失败!" + e.getMessage());
                }
            } else {
                throw new BusinessException("代码类型名：数据质量校验结果类型不合法!");
            }
        }
        return check_index3_list;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<EtlSys> getProInfos() {
        return EtlJobUtil.getProInfo(Dbo.db(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<EtlSubSysList> getTaskInfo(Long etl_sys_id) {
        return SqlOperator.queryList(Dbo.db(), EtlSubSysList.class, "select * from " + EtlSubSysList.TableName + " where" + " etl_sys_id =? order by sub_sys_id", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "reg_num", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> viewRuleSchedulingStatus(long reg_num) {
        DqDefinition dqd = new DqDefinition();
        dqd.setReg_num(reg_num);
        DqDefinition dq_definition = getDqDefinition(dqd.getReg_num());
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT etl_sys_id,sub_sys_id,etl_job,job_eff_flag,job_disp_status FROM " + EtlJobDef.TableName + " where").addSql(" 1=1 ").addLikeParam("etl_job", '%' + dq_definition.getReg_num().toString() + '%');
        asmSql.addSql("UNION");
        asmSql.addSql("SELECT etl_sys_id,sub_sys_id,etl_job,job_eff_flag,job_disp_status FROM " + EtlJobCur.TableName + " WHERE").addSql(" 1=1 ").addLikeParam("etl_job", '%' + dq_definition.getReg_num().toString() + '%');
        asmSql.addSql("UNION");
        asmSql.addSql("SELECT etl_sys_id,sub_sys_id,etl_job,job_eff_flag,job_disp_status FROM " + EtlJobDispHis.TableName + " WHERE").addSql(" 1=1 ").addLikeParam("etl_job", '%' + dq_definition.getReg_num().toString() + '%');
        List<Map<String, Object>> queryList = Dbo.queryList(asmSql.sql(), asmSql.params());
        List<Map<String, Object>> ruleSchedulingStatusInfos = new ArrayList<>();
        queryList.forEach(o -> {
            Map<String, Object> map = new HashMap<>();
            map.put("etl_sys_id", o.get("etl_sys_id").toString());
            map.put("sub_sys_id", o.get("sub_sys_id").toString());
            map.put("etl_job", o.get("etl_job").toString());
            map.put("job_eff_flag", o.get("job_eff_flag"));
            map.put("job_disp_status", o.get("job_disp_status"));
            map.put("reg_num", dq_definition.getReg_num());
            map.put("case_type", dq_definition.getCase_type());
            map.put("target_tab", dq_definition.getTarget_tab());
            ruleSchedulingStatusInfos.add(map);
        });
        return ruleSchedulingStatusInfos;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dq_definition", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public Map<String, Object> specifySqlCheck(DqDefinition dq_definition) {
        if (StringUtil.isBlank(dq_definition.getSpecify_sql())) {
            throw new BusinessException("指定SQL为空!");
        }
        if (StringUtil.isNotEmpty(dq_definition.getErr_data_sql())) {
            dq_definition.setErr_data_sql(AesUtil.desEncrypt(dq_definition.getErr_data_sql()));
        }
        if (StringUtil.isNotEmpty(dq_definition.getSpecify_sql())) {
            dq_definition.setSpecify_sql(AesUtil.desEncrypt(dq_definition.getSpecify_sql()));
        }
        Set<SysVarCheckBean> beans = DqcExecution.getSysVarCheckBean(Dbo.db(), dq_definition);
        String check_is_success = DqcExecution.executionSqlCheck(beans, dq_definition.getSpecify_sql().split(";"));
        Map<String, Object> specifySqlCheckMap = new HashMap<>();
        specifySqlCheckMap.put("sysVarCheckBean", beans);
        specifySqlCheckMap.put("check_is_success", check_is_success);
        return specifySqlCheckMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dq_definition", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public Map<String, Object> errDataSqlCheck(DqDefinition dq_definition) {
        if (StringUtil.isBlank(dq_definition.getErr_data_sql())) {
            throw new BusinessException("指定SQL为空!");
        }
        dq_definition.setErr_data_sql(AesUtil.desEncrypt(dq_definition.getErr_data_sql()));
        dq_definition.setSpecify_sql(AesUtil.desEncrypt(dq_definition.getSpecify_sql()));
        Set<SysVarCheckBean> beans = DqcExecution.getSysVarCheckBean(Dbo.db(), dq_definition);
        String check_is_success = DqcExecution.executionSqlCheck(beans, dq_definition.getErr_data_sql());
        Map<String, Object> specifySqlCheckMap = new HashMap<>();
        specifySqlCheckMap.put("sysVarCheckBean", beans);
        specifySqlCheckMap.put("check_is_success", check_is_success);
        return specifySqlCheckMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "reg_num", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean checkRegNumIsExist(long reg_num) {
        return Dbo.queryNumber("SELECT COUNT(reg_num) FROM " + DqDefinition.TableName + " WHERE reg_num = ?", reg_num).orElseThrow(() -> new BusinessException("检查规则reg_num否存在的SQL错误")) == 1;
    }
}
