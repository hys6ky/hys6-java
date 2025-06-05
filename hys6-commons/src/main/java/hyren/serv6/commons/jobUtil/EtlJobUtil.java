package hyren.serv6.commons.jobUtil;

import com.jcraft.jsch.JSchException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.PinyinUtil;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.jobUtil.beans.EtlJobInfo;
import hyren.serv6.commons.jobUtil.beans.JobStartConf;
import hyren.serv6.commons.jobUtil.beans.ObjJobBean;
import hyren.serv6.commons.jobUtil.dcletljob.DclEtlJobUtil;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@DocClass(desc = "", author = "BY-HLL", createdate = "2020/4/8 0008 下午 01:38")
public class EtlJobUtil {

    private static final String ETL_SYS_CD = "HYREN";

    private static final String ETL_SYS_NAME = "海云数服相关作业";

    private static final int RESOURCE_MAX = 5;

    private static final String BATCH_DATE = "#{txdate}";

    public static final String QUALITY_MANAGE = "quality_manage.sh";

    public static final String MARTPRONAME = "process-job-command.sh";

    @Method(desc = "", logicStep = "")
    @Param(name = "pkId", desc = "", range = "")
    @Param(name = "dataSourceType", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    @Param(name = "collectType", desc = "", range = "")
    public static int saveJob(String pkId, DataSourceType dataSourceType, Long etl_sys_id, Long sub_sys_id, AgentType agentType) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
            if (dataSourceType == DataSourceType.DCL) {
                if (agentType == AgentType.FTP) {
                    saveFtpJob(pkId, dataSourceType, etl_sys_id, sub_sys_id, asmSql, db);
                }
                if (agentType == AgentType.DuiXiang) {
                    throw new BusinessException("暂未实现!");
                }
                if (agentType == AgentType.WenJianXiTong) {
                    throw new BusinessException("暂未实现!");
                }
                if (agentType == AgentType.ShuJuKu || agentType == AgentType.DBWenJian) {
                    DatabaseSet database_set = new DatabaseSet();
                    database_set.setDatabase_id(Long.parseLong(pkId));
                    String jobDataSource = getJobDataSource(db, database_set.getDatabase_id());
                    List<Map<String, Object>> previewJob = DclEtlJobUtil.getPreviewJob(database_set.getDatabase_id(), db);
                    List<EtlJobDef> etl_job_defs = DclEtlJobUtil.setDefaultEtlJob(etl_sys_id, sub_sys_id, previewJob, jobDataSource);
                    Map<String, Object> agentPath = DclEtlJobUtil.getAgentPath(database_set.getDatabase_id(), db);
                    List<Long> dedList = previewJob.stream().map(item -> Long.parseLong(item.get("ded_id").toString())).collect(Collectors.toList());
                    EtlJobInfo etlJobInfo = new EtlJobInfo();
                    etlJobInfo.setDedIds(dedList);
                    etlJobInfo.setEtlJobs(etl_job_defs);
                    etlJobInfo.setPro_dic(agentPath.get("pro_dic").toString());
                    if (StringUtil.isNotEmpty(agentPath.get("log_dic").toString()) && agentPath.get("log_dic").toString().contains(".log")) {
                        etlJobInfo.setLog_dic(agentPath.get("log_dic").toString().substring(0, agentPath.get("log_dic").toString().lastIndexOf("/")));
                    } else {
                        etlJobInfo.setLog_dic(agentPath.get("log_dic").toString());
                    }
                    etlJobInfo.setSource_id(Long.parseLong(agentPath.get("source_id").toString()));
                    etlJobInfo.setColSetId(database_set.getDatabase_id());
                    etlJobInfo.setEtl_sys_id(etl_sys_id);
                    etlJobInfo.setSub_sys_id(sub_sys_id);
                    DclEtlJobUtil.saveJobDataToDatabase(etlJobInfo, db);
                }
            }
            if (dataSourceType == DataSourceType.DPL) {
                throw new BusinessException("暂未实现!");
            }
            if (dataSourceType == DataSourceType.DML) {
                asmSql.clean();
                DmModuleTable dmModuleTable = new DmModuleTable();
                dmModuleTable.setModule_table_id(Long.parseLong(pkId));
                asmSql.addSql("SELECT * FROM " + DmModuleTable.TableName + " WHERE module_table_id = ?");
                asmSql.addParam(dmModuleTable.getModule_table_id());
                dmModuleTable = SqlOperator.queryOneObject(db, DmModuleTable.class, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("获取规则校验信息的SQL出错!")));
                String etl_job = dmModuleTable.getModule_table_en_name();
                String etl_job_desc = dmModuleTable.getModule_table_en_name();
                String param = pkId + "@" + BATCH_DATE;
                EtlSubSysList etlSubSysList = SqlOperator.queryOneObject(db, EtlSubSysList.class, "select * from " + EtlSubSysList.TableName + " where sub_sys_id=? ", sub_sys_id).orElseThrow(() -> (new BusinessException("未找到对应的子系统")));
                jobCommonMethod(etlSubSysList.getSub_sys_cd(), etlSubSysList.getSub_sys_cd(), etl_job, etl_job_desc, param, dataSourceType, MARTPRONAME, etl_sys_id, sub_sys_id, ETLDataSource.ShuJuJiaGong.getCode(), pkId, db);
            }
            if (dataSourceType == DataSourceType.DQC) {
            }
            db.commit();
            return 0;
        } catch (Exception e) {
            log.error("保存作业失败：", e);
            return -1;
        }
    }

    private static void saveFtpJob(String pkId, DataSourceType dataSourceType, Long etl_sys_id, Long sub_sys_id, SqlOperator.Assembler asmSql, DatabaseWrapper db) {
        asmSql.clean();
        FtpCollect ftpCollect = new FtpCollect();
        ftpCollect.setFtp_id(pkId);
        asmSql.addSql("SELECT * FROM " + FtpCollect.TableName + " WHERE ftp_id = ?");
        asmSql.addParam(ftpCollect.getFtp_id());
        ftpCollect = SqlOperator.queryOneObject(db, FtpCollect.class, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("获取规则校验信息的SQL出错!")));
        String etl_job = ftpCollect.getFtp_name();
        String etl_job_desc = ftpCollect.getFtp_name();
        EtlSubSysList etlSubSysList = SqlOperator.queryOneObject(db, EtlSubSysList.class, "select * from " + EtlSubSysList.TableName + " where sub_sys_id=? ", sub_sys_id).orElseThrow(() -> (new BusinessException("未找到对应的子系统")));
        jobCommonMethod(etlSubSysList.getSub_sys_cd(), etlSubSysList.getSub_sys_cd(), etl_job, etl_job_desc, pkId, dataSourceType, Constant.FTP_JOB_COMMAND, etl_sys_id, sub_sys_id, ETLDataSource.Other.getCode(), pkId, db);
    }

    @Method(desc = "", logicStep = "")
    public static void saveObjJob(ObjJobBean objJobBean, DatabaseWrapper db) {
        Long odc_id = objJobBean.getOdc_id();
        Validator.notNull(odc_id, "对象任务id不能为空");
        SqlOperator.execute(db, "DELETE FROM " + EtlJobDef.TableName + " WHERE etl_job_id in" + " (SELECT t2.etl_job_id from " + TakeRelationEtl.TableName + " t1 JOIN " + EtlJobDef.TableName + " t2 ON t1.etl_job_id = t2.etl_job_id " + " WHERE t1.etl_sys_id = t2.etl_sys_id " + " AND t1.sub_sys_id = t2.sub_sys_id AND t1.take_id = ?)", odc_id);
        for (EtlJobDef etl_job_def : objJobBean.getEtlJobDefs()) {
            Validator.notBlank(etl_job_def.getEtl_job(), "作业名称不能为空!!!");
            Long etlSysId = etl_job_def.getEtl_sys_id();
            Validator.notNull(etlSysId, "系统ID不能为空!!!");
            Validator.notNull(etl_job_def.getSub_sys_id(), "子系统不能为空!!!");
            Validator.notBlank(etl_job_def.getPro_type(), "作业程序类型不能为空!!!");
            EtlJobUtil.setDefaultEtlParaConf(Dbo.db(), etlSysId, Constant.PARA_HYRENBIN, etl_job_def.getPro_dic() + File.separator);
            EtlJobUtil.setDefaultEtlParaConf(Dbo.db(), etlSysId, Constant.PARA_HYRENLOG, etl_job_def.getLog_dic());
            EtlJobUtil.setDefaultEtlResource(Dbo.db(), etlSysId);
            List<Long> jobResource = EtlJobUtil.getJobResource(Dbo.db(), etlSysId);
            List<Long> etlJobList = EtlJobUtil.getEtlJob(Dbo.db(), etlSysId, etl_job_def.getSub_sys_id());
            etl_job_def.setPro_dic(etl_job_def.getPro_dic() + File.separator);
            etl_job_def.setLog_dic(Constant.HYRENLOG);
            etl_job_def.setPro_name(Constant.SEMISTRUCTURED_JOB_COMMAND);
            etl_job_def.setJob_eff_flag(Job_Effective_Flag.YES.getCode());
            etl_job_def.setToday_disp(Today_Dispatch_Flag.YES.getCode());
            etl_job_def.setUpd_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
            if (etlJobList.contains(etl_job_def.getEtl_job_id())) {
                try {
                    etl_job_def.update(Dbo.db());
                } catch (Exception e) {
                    if (!(e instanceof ProEntity.EntityDealZeroException)) {
                        throw new BusinessException(e.getMessage());
                    }
                }
            } else {
                etl_job_def.setEtl_job_id(PrimayKeyGener.getNextId());
                etl_job_def.add(Dbo.db());
            }
            EtlJobUtil.setEtl_job_resource_rela(Dbo.db(), etlSysId, etl_job_def, jobResource);
            List<String> relationEtl = getObjRelationEtl(odc_id);
            for (JobStartConf jobStartConf : objJobBean.getJobStartConfs()) {
                Long[] preEtlJobIds = jobStartConf.getPre_etl_job_ids();
                if (preEtlJobIds != null && preEtlJobIds.length > 0) {
                    saveEtlDependencies(etlSysId, jobStartConf.getEtl_job_id(), preEtlJobIds);
                }
                addObjRelationEtl(etl_job_def, relationEtl, odc_id);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_job_def", desc = "", range = "", isBean = true)
    @Param(name = "relationEtl", desc = "", range = "")
    @Param(name = "odc_id", desc = "", range = "")
    private static void addObjRelationEtl(EtlJobDef etl_job_def, List<String> relationEtl, long odc_id) {
        if (!relationEtl.contains(etl_job_def.getEtl_job())) {
            TakeRelationEtl take_relation_etl = new TakeRelationEtl();
            take_relation_etl.setTre_id(PrimayKeyGener.getNextId());
            take_relation_etl.setTake_id(odc_id);
            Validator.notNull(etl_job_def.getEtl_job_id(), "作业主键ID不能为空");
            Validator.notNull(etl_job_def.getEtl_sys_id(), "作业系统ID不能为空");
            Validator.notNull(etl_job_def.getSub_sys_id(), "作业子系统ID不能为空");
            take_relation_etl.setEtl_job_id(etl_job_def.getEtl_job_id());
            take_relation_etl.setEtl_sys_id(etl_job_def.getEtl_sys_id());
            take_relation_etl.setSub_sys_id(etl_job_def.getSub_sys_id());
            take_relation_etl.setJob_datasource(ETLDataSource.BanJieGouHuaCaiJi.getCode());
            take_relation_etl.setTake_source_table(ObjectCollectTask.TableName);
            take_relation_etl.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    private static List<String> getObjRelationEtl(long odc_id) {
        return Dbo.queryOneColumnList("SELECT t1.etl_job_id FROM " + TakeRelationEtl.TableName + " t1 JOIN " + ObjectCollectTask.TableName + " t2 ON t1.take_id = t2.ocs_id JOIN " + ObjectCollect.TableName + " t3 ON t2.odc_id = t3.odc_id JOIN " + AgentInfo.TableName + " t4 ON t3.agent_id = t4.agent_id JOIN " + DataSource.TableName + " t5 ON t4.source_id = t5.source_id" + " WHERE t2.odc_id = ?", odc_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "status", desc = "", range = "")
    @Param(name = "pre_etl_job_ids", desc = "", range = "")
    private static void saveEtlDependencies(long etl_sys_id, long etl_job_id, Long[] pre_etl_job_ids) {
        Dbo.execute("DELETE FROM " + EtlDependency.TableName + " WHERE (etl_job_id = ? OR pre_etl_job_id = ?) AND etl_sys_id = ? ", etl_job_id, etl_job_id, etl_sys_id);
        List<Object[]> etlDepList = new ArrayList<>();
        if (pre_etl_job_ids != null && pre_etl_job_ids.length != 0) {
            for (long preEtlJob : pre_etl_job_ids) {
                Object[] objects = new Object[6];
                objects[0] = etl_sys_id;
                objects[1] = etl_sys_id;
                objects[2] = etl_job_id;
                objects[3] = preEtlJob;
                objects[4] = Status.TRUE.getCode();
                objects[5] = Main_Server_Sync.YES.getCode();
                etlDepList.add(objects);
            }
            Dbo.executeBatch("insert into " + EtlDependency.TableName + "(ETL_SYS_ID,PRE_ETL_SYS_ID,ETL_JOB_ID,PRE_ETL_JOB_ID,status,main_serv_sync)" + " values(?,?,?,?,?,?)", etlDepList);
        }
    }

    public static String getJobDataSource(DatabaseWrapper db, long database_id) {
        String job_datasource;
        DatabaseSet database_set = SqlOperator.queryOneObject(db, DatabaseSet.class, "select collect_type,agent_id from " + DatabaseSet.TableName + " where database_id = ?", database_id).orElseThrow(() -> new BusinessException("sql查询错误！"));
        AgentInfo agent_info = SqlOperator.queryOneObject(db, AgentInfo.class, "select agent_type from " + AgentInfo.TableName + " where agent_id = ?", database_set.getAgent_id()).orElseThrow(() -> new BusinessException("sql查询错误！"));
        if (AgentType.DBWenJian == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
            job_datasource = ETLDataSource.DBWenJianCaiJi.getCode();
        } else if (AgentType.DuiXiang == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
            job_datasource = ETLDataSource.BanJieGouHuaCaiJi.getCode();
        } else if (AgentType.WenJianXiTong == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
            job_datasource = ETLDataSource.FeiJieGouHuaCaiJi.getCode();
        } else if (AgentType.ShuJuKu == AgentType.ofEnumByCode(agent_info.getAgent_type())) {
            if (CollectType.ShuJuKuChouShu == CollectType.ofEnumByCode(database_set.getCollect_type())) {
                job_datasource = ETLDataSource.ShuJuKuChouShu.getCode();
            } else if (CollectType.ShuJuKuCaiJi == CollectType.ofEnumByCode(database_set.getCollect_type())) {
                job_datasource = ETLDataSource.ShuJuKuCaiJi.getCode();
            } else {
                job_datasource = ETLDataSource.Other.getCode();
            }
        } else {
            job_datasource = ETLDataSource.Other.getCode();
        }
        return job_datasource;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sub_name", desc = "", range = "")
    @Param(name = "sub_sys_desc", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    @Param(name = "etl_job_desc", desc = "", range = "")
    @Param(name = "param", desc = "", range = "")
    @Param(name = "dataSourceType", desc = "", range = "")
    @Param(name = "pro_name", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    public static void jobCommonMethod(String sub_name, String sub_sys_desc, String etl_job, String etl_job_desc, String param, DataSourceType dataSourceType, String pro_name, Long etl_sys_id, Long sub_sys_id, String job_datasource, String pkId, DatabaseWrapper db) {
        createEtl_sys(db, etl_sys_id, Long.parseLong(pkId));
        String pro_dic = "!{HYSHELLBIN}";
        if (dataSourceType == DataSourceType.DCL) {
            EtlSubSysList etl_sub_sys_list = new EtlSubSysList();
            etl_sub_sys_list.setEtl_sys_id(etl_sys_id);
            etl_sub_sys_list.setSub_sys_id(sub_sys_id);
            etl_sub_sys_list.setSub_sys_cd(sub_name);
            etl_sub_sys_list.setSub_sys_desc(sub_sys_desc);
            if (sub_sys_id == null) {
                sub_sys_id = createEtl_sub_sys_list(etl_sub_sys_list, db);
            }
            createEtl_job_def(etl_sys_id, sub_sys_id, etl_job, etl_job_desc, param, pro_name, db, pro_dic, job_datasource);
        } else if (dataSourceType == DataSourceType.DPL) {
            String subSysCd = new PinyinUtil().toFixPinYin(sub_name) + "_" + dataSourceType.getCode();
            EtlSubSysList etl_sub_sys_list = new EtlSubSysList();
            etl_sub_sys_list.setEtl_sys_id(etl_sys_id);
            etl_sub_sys_list.setSub_sys_id(sub_sys_id);
            etl_sub_sys_list.setSub_sys_id(subSysCd);
            etl_sub_sys_list.setSub_sys_desc(sub_sys_desc);
            if (sub_sys_id == null) {
                sub_sys_id = createEtl_sub_sys_list(etl_sub_sys_list, db);
            }
            createEtl_job_def(etl_sys_id, sub_sys_id, etl_job, etl_job_desc, param, pro_name, db, pro_dic, job_datasource);
        } else if (dataSourceType == DataSourceType.DML) {
            String subSysCd = new PinyinUtil().toFixPinYin(sub_name) + "_" + dataSourceType.getCode();
            EtlSubSysList etl_sub_sys_list = new EtlSubSysList();
            etl_sub_sys_list.setEtl_sys_id(etl_sys_id);
            etl_sub_sys_list.setSub_sys_id(sub_sys_id);
            etl_sub_sys_list.setSub_sys_cd(subSysCd);
            etl_sub_sys_list.setSub_sys_desc(sub_sys_desc);
            if (sub_sys_id == null) {
                sub_sys_id = createEtl_sub_sys_list(etl_sub_sys_list, db);
            }
            EtlJobDef etlJobDef = createEtl_job_def(etl_sys_id, sub_sys_id, subSysCd + "_" + etl_job, etl_job_desc, param, pro_name, db, pro_dic, job_datasource);
            long datatableId = Long.parseLong(pkId);
            List<Long> relationEtl = SqlOperator.queryOneColumnList(db, "select etl_job_id from " + TakeRelationEtl.TableName + " tre" + " JOIN " + DmModuleTable.TableName + " dd ON tre.take_id = dd.module_table_id" + " WHERE dd.module_table_id = ?", datatableId);
            DclEtlJobUtil.setTake_relation_etl(etlJobDef, relationEtl, datatableId, db);
        } else if (dataSourceType == DataSourceType.DQC) {
            String subSysCd = new PinyinUtil().toFixPinYin(sub_name) + "_" + dataSourceType.getCode();
            EtlSubSysList etl_sub_sys_list = new EtlSubSysList();
            etl_sub_sys_list.setEtl_sys_id(etl_sys_id);
            etl_sub_sys_list.setSub_sys_id(sub_sys_id);
            etl_sub_sys_list.setSub_sys_cd(subSysCd);
            etl_sub_sys_list.setSub_sys_desc(sub_sys_desc);
            if (sub_sys_id == null) {
                sub_sys_id = createEtl_sub_sys_list(etl_sub_sys_list, db);
            }
            pro_dic = WebinfoProperties.FileUpload_SavedDirName + File.separator + "pro_dic" + File.separator;
            EtlJobDef etlJobDef = createEtl_job_def(etl_sys_id, sub_sys_id, subSysCd + "_" + etl_job, etl_job_desc, param, pro_name, db, pro_dic, job_datasource);
            long table_id = Long.parseLong(pkId);
            List<Long> relationEtl = SqlOperator.queryOneColumnList(db, "select etl_job_id from " + TakeRelationEtl.TableName + " tre" + " JOIN " + DqTableInfo.TableName + " dti ON tre.take_id = dti.table_id" + " WHERE dti.table_id = ?", table_id);
            DclEtlJobUtil.setTake_relation_etl(etlJobDef, relationEtl, table_id, db);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    @Param(name = "etl_job_desc", desc = "", range = "")
    @Param(name = "param", desc = "", range = "")
    @Param(name = "pro_name", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "pro_dic", desc = "", range = "")
    private static EtlJobDef createEtl_job_def(Long etl_sys_id, Long sub_sys_id, String etl_job, String etl_job_desc, String param, String Pro_name, DatabaseWrapper db, String pro_dic, String job_datasource) {
        EtlJobDef etl_job_def = new EtlJobDef();
        etl_job_def.setEtl_sys_id(etl_sys_id);
        etl_job_def.setEtl_job(etl_job);
        etl_job_def.setSub_sys_id(sub_sys_id);
        etl_job_def.setEtl_job_desc(etl_job_desc);
        etl_job_def.setPro_type(Pro_Type.SHELL.getCode());
        etl_job_def.setPro_dic(pro_dic);
        etl_job_def.setPro_name(Pro_name);
        etl_job_def.setPro_para(param);
        etl_job_def.setLog_dic("!{HYLOG}@" + BATCH_DATE + "@!{HYXX}");
        etl_job_def.setDisp_freq(Dispatch_Frequency.DAILY.getCode());
        etl_job_def.setDisp_offset(0);
        etl_job_def.setDisp_type(Dispatch_Type.TPLUS0.getCode());
        etl_job_def.setJob_eff_flag(Job_Effective_Flag.YES.getCode());
        etl_job_def.setJob_priority(0);
        etl_job_def.setDisp_time("00:00:00");
        etl_job_def.setToday_disp(Today_Dispatch_Flag.YES.getCode());
        etl_job_def.setMain_serv_sync(Main_Server_Sync.YES.getCode());
        etl_job_def.setJob_datasource(job_datasource);
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select count(*) from " + EtlJobDef.TableName + " where etl_sys_id = ? and etl_job = ?");
        asmSql.addParam(etl_job_def.getEtl_sys_id());
        asmSql.addParam(etl_job_def.getEtl_job());
        if (SqlOperator.queryNumber(db, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("检查工程作业定义信息的SQL错误!"))) == 1) {
            throw new BusinessException(String.format("作业%s已经存在于该工程!", etl_job_def.getEtl_job()));
        } else {
            etl_job_def.setEtl_job_id(PrimayKeyGener.getNextId());
            etl_job_def.add(db);
            EtlJobResourceRela etl_job_resource_rela = new EtlJobResourceRela();
            etl_job_resource_rela.setEtl_sys_id(etl_sys_id);
            etl_job_resource_rela.setEtl_job_id(etl_job_def.getEtl_job_id());
            etl_job_resource_rela.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
            etl_job_resource_rela.setResource_req(1);
            asmSql.clean();
            asmSql.addSql("select count(*) from " + EtlJobResourceRela.TableName + " where etl_sys_id = ? and etl_job_id = ?");
            asmSql.addParam(etl_job_resource_rela.getEtl_sys_id());
            asmSql.addParam(etl_job_resource_rela.getEtl_job_id());
            if (SqlOperator.queryNumber(db, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("检查工程作业资源信息的SQL错误!"))) != 1) {
                etl_job_resource_rela.add(db);
            }
        }
        return etl_job_def;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "relation", desc = "", range = "")
    @Param(name = "dd_ids", desc = "", range = "")
    @Param(name = "pre_etl_job_ids", desc = "", range = "", nullable = true)
    public static void saveDistributeDataJob(List<EtlJobDef> relation, List<Map<String, String>> dd_ids, List<List<Long>> pre_etl_job_ids) {
        for (int i = 0; i < relation.size(); i++) {
            EtlJobDef etlJobDef = new EtlJobDef();
            BeanUtil.copyProperties(relation.get(i), etlJobDef);
            etlJobDef.setPro_type(Pro_Type.SHELL.getCode());
            etlJobDef.setPro_name(Constant.DISTRIBUTE_JOB_COMMAND);
            etlJobDef.setJob_datasource(ETLDataSource.ShuJuFenFa.getCode());
            etlJobDef.setPro_para(dd_ids.get(i).get("dd_id") + Constant.ETLPARASEPARATOR + "#{txdate}");
            etlJobDef.setJob_eff_flag(Job_Effective_Flag.YES.getCode());
            etlJobDef.setToday_disp(Today_Dispatch_Flag.YES.getCode());
            etlJobDef.setUpd_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
            Optional<EtlJobDef> etl_job_def = Dbo.queryOneObject(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where etl_job_id = ? ", relation.get(i).getEtl_job_id());
            if (etl_job_def.isPresent()) {
                EtlJobDef jobDef = etl_job_def.get();
                int job_ret = Dbo.execute("delete from " + EtlJobDef.TableName + " where etl_job_id = ? and etl_sys_id = ?", jobDef.getEtl_job_id(), jobDef.getEtl_sys_id());
                if (job_ret < 1) {
                    throw new BusinessException("删除作业失败!");
                }
                long num = Dbo.queryNumber("select count(1) from " + TakeRelationEtl.TableName + " where etl_job_id = ?", jobDef.getEtl_job_id()).orElseThrow(() -> new BusinessException("sql查询任务作业关系表信息错误"));
                if (num > 0) {
                    int execute = Dbo.execute("delete from " + TakeRelationEtl.TableName + " where etl_job_id = ?", jobDef.getEtl_job_id());
                    if (execute < 1) {
                        throw new BusinessException("删除作业分发数据关系表失败!");
                    }
                }
                long count = Dbo.queryNumber("select count(pre_etl_job_id) from " + EtlDependency.TableName + " where etl_job_id = ? and etl_sys_id = ?", jobDef.getEtl_job_id(), jobDef.getEtl_sys_id()).orElseThrow(() -> new BusinessException("sql查询错误"));
                if (count > 0) {
                    int ret = Dbo.execute("delete from " + EtlDependency.TableName + " where etl_job_id = ? and etl_sys_id = ?", jobDef.getEtl_job_id(), jobDef.getEtl_sys_id());
                    if (ret < 1) {
                        throw new BusinessException("删除上游依赖关系失败!");
                    }
                }
            }
            etlJobDef.add(Dbo.db());
            EtlResource etl_resource = new EtlResource();
            etl_resource.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            etl_resource.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
            etl_resource.setResource_max(RESOURCE_MAX);
            etl_resource.setMain_serv_sync(Main_Server_Sync.YES.getCode());
            if (Dbo.queryNumber("select count(*) from " + EtlResource.TableName + " where etl_sys_id = ? and resource_type = ?", etl_resource.getEtl_sys_id(), etl_resource.getResource_type()).orElseThrow(() -> (new BusinessException("检查参数登记信息的SQL错误!"))) != 1) {
                etl_resource.add(Dbo.db());
            }
            EtlJobResourceRela etlJobResourceRela = new EtlJobResourceRela();
            etlJobResourceRela.setEtl_job_id(etlJobDef.getEtl_job_id());
            etlJobResourceRela.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            etlJobResourceRela.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
            etlJobResourceRela.setResource_req(1);
            if (Dbo.queryNumber("select count(*) from " + EtlJobResourceRela.TableName + " where etl_job_id = ? and resource_type = ?", etlJobResourceRela.getEtl_job_id(), etlJobResourceRela.getResource_type()).orElseThrow(() -> (new BusinessException("检查参数登记信息的SQL错误!"))) != 1) {
                etlJobResourceRela.add(Dbo.db());
            }
            TakeRelationEtl takeRelationEtl = new TakeRelationEtl();
            takeRelationEtl.setTre_id(PrimayKeyGener.getNextId());
            takeRelationEtl.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            takeRelationEtl.setSub_sys_id(etlJobDef.getSub_sys_id());
            takeRelationEtl.setEtl_job_id(etlJobDef.getEtl_job_id());
            takeRelationEtl.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            takeRelationEtl.setTake_id(dd_ids.get(i).get("dd_id"));
            takeRelationEtl.setJob_datasource(ETLDataSource.ShuJuFenFa.getCode());
            takeRelationEtl.setTake_source_table(DataDistribute.TableName);
            takeRelationEtl.add(Dbo.db());
            EtlDependency etl_dependency = new EtlDependency();
            etl_dependency.setEtl_job_id(etlJobDef.getEtl_job_id());
            etl_dependency.setEtl_sys_id(etlJobDef.getEtl_sys_id());
            etl_dependency.setStatus(Status.TRUE.getCode());
            etl_dependency.setMain_serv_sync(Main_Server_Sync.YES.getCode());
            etl_dependency.setPre_etl_sys_id(etlJobDef.getEtl_sys_id());
            if (Dispatch_Type.DEPENDENCE == Dispatch_Type.ofEnumByCode(etlJobDef.getDisp_type()) && (!CollectionUtils.isEmpty(pre_etl_job_ids))) {
                for (Long pre_etl_job_id : pre_etl_job_ids.get(i)) {
                    etl_dependency.setPre_etl_job_id(pre_etl_job_id);
                    etl_dependency.add(Dbo.db());
                }
            }
            int ret = Dbo.execute("update " + DataDistribute.TableName + " set is_release = '1' where dd_id = ?", Long.parseLong(dd_ids.get(i).get("dd_id")));
            if (ret != 1) {
                throw new BusinessException("修改发布状态失败!");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sub_sys_list", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    private static Long createEtl_sub_sys_list(EtlSubSysList etl_sub_sys_list, DatabaseWrapper db) {
        if (etl_sub_sys_list.getEtl_sys_id() == null) {
            throw new BusinessException("工程代码为空!");
        }
        if (etl_sub_sys_list.getSub_sys_id() == null) {
            throw new BusinessException("子系统代码为空!");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select count(1) count from " + EtlSubSysList.TableName + " where etl_sys_id = ? and sub_sys_id = ?");
        asmSql.addParam(etl_sub_sys_list.getEtl_sys_id());
        asmSql.addParam(etl_sub_sys_list.getSub_sys_id());
        if (SqlOperator.queryNumber(db, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("检查工程子系统定义信息的SQL错误!"))) != 1) {
            etl_sub_sys_list.add(db);
        }
        return etl_sub_sys_list.getSub_sys_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "ftp_id", desc = "", range = "")
    private static void createEtl_sys(DatabaseWrapper db, Long etl_sys_id, Long ftp_id) {
        Map<String, Object> agentPath = getFtpAgentPath(ftp_id, db);
        String pro_path = agentPath.get("pro_dic").toString() + File.separator;
        String log_path;
        if (StringUtil.isNotEmpty(agentPath.get("log_dic").toString()) && agentPath.get("log_dic").toString().contains(".log")) {
            log_path = agentPath.get("log_dic").toString().substring(0, agentPath.get("log_dic").toString().lastIndexOf("/")) + File.separator;
        } else {
            log_path = agentPath.get("log_dic").toString() + File.separator;
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + EtlSys.TableName + " where etl_sys_id = ?");
        asmSql.addParam(etl_sys_id);
        EtlSys etlSys = SqlOperator.queryOneObject(db, EtlSys.class, asmSql.sql(), asmSql.params()).orElseThrow(() -> new AppSystemException("获取作业系统信息失败"));
        if (ETL_SYS_CD.equals(etlSys.getEtl_sys_cd())) {
            asmSql.clean();
            asmSql.addSql("select count(1) count from etl_sys where etl_sys_id = ?");
            asmSql.addParam(etlSys.getEtl_sys_id());
            long num = SqlOperator.queryNumber(db, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("检查工程是否存在的SQL错误!")));
            if (num == 0) {
                asmSql.clean();
                EtlSys etl_sys = new EtlSys();
                etl_sys.setEtl_sys_id(PrimayKeyGener.getNextId());
                etl_sys.setEtl_sys_cd(ETL_SYS_CD);
                etl_sys.setEtl_sys_name(ETL_SYS_NAME);
                etl_sys.setEtl_serv_ip(PropertyParaValue.getString("hyren_host", ""));
                etl_sys.setEtl_serv_port(PropertyParaValue.getString("hyren_port", ""));
                etl_sys.setMain_serv_sync(Main_Server_Sync.YES.toString());
                etl_sys.setSys_run_status(Job_Status.STOP.toString());
                etl_sys.setCurr_bath_date(DateUtil.getSysDate());
                etl_sys.add(db);
                asmSql.clean();
                EtlPara etl_para = new EtlPara();
                etl_para.setEtl_sys_id(etlSys.getEtl_sys_id());
                etl_para.setPara_cd("!HYSHELLBIN");
                etl_para.setPara_val(pro_path);
                etl_para.setPara_type("url");
                etl_para.add(db);
                etl_para.setEtl_sys_id(etlSys.getEtl_sys_id());
                etl_para.setPara_cd("!HYXX");
                etl_para.setPara_val("/");
                etl_para.setPara_type("url");
                etl_para.add(db);
                etl_para.setEtl_sys_id(etlSys.getEtl_sys_id());
                etl_para.setPara_cd("!HYLOG");
                etl_para.setPara_val(log_path);
                etl_para.setPara_type("url");
                etl_para.add(db);
                EtlResource etl_resource = new EtlResource();
                etl_resource.setEtl_sys_id(etlSys.getEtl_sys_id());
                etl_resource.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
                etl_resource.setResource_max(RESOURCE_MAX);
                etl_resource.setMain_serv_sync(Main_Server_Sync.YES.toString());
                etl_resource.add(db);
            }
        } else {
            EtlPara etl_para = new EtlPara();
            etl_para.setEtl_sys_id(etlSys.getEtl_sys_id());
            etl_para.setPara_cd("!HYSHELLBIN");
            etl_para.setPara_val(pro_path);
            etl_para.setPara_type("url");
            asmSql.clean();
            asmSql.addSql("select count(*) from etl_para where etl_sys_id = ? and para_cd = ?");
            asmSql.addParam(etlSys.getEtl_sys_id());
            asmSql.addParam(etl_para.getPara_cd());
            if (SqlOperator.queryNumber(db, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("检查参数登记信息的SQL错误!"))) != 1) {
                etl_para.add(db);
            }
            etl_para.setEtl_sys_id(etlSys.getEtl_sys_id());
            etl_para.setPara_cd("!HYXX");
            etl_para.setPara_val("/");
            etl_para.setPara_type("url");
            asmSql.cleanParams();
            asmSql.addParam(etlSys.getEtl_sys_id());
            asmSql.addParam(etl_para.getPara_cd());
            if (SqlOperator.queryNumber(db, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("检查参数登记信息的SQL错误!"))) != 1) {
                etl_para.add(db);
            }
            etl_para.setEtl_sys_id(etlSys.getEtl_sys_id());
            etl_para.setPara_cd("!HYLOG");
            etl_para.setPara_val(log_path);
            etl_para.setPara_type("url");
            asmSql.cleanParams();
            asmSql.addParam(etlSys.getEtl_sys_id());
            asmSql.addParam(etl_para.getPara_cd());
            if (SqlOperator.queryNumber(db, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("检查参数登记信息的SQL错误!"))) != 1) {
                etl_para.add(db);
            }
            asmSql.clean();
            EtlResource etl_resource = new EtlResource();
            etl_resource.setEtl_sys_id(etlSys.getEtl_sys_id());
            etl_resource.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
            etl_resource.setResource_max(RESOURCE_MAX);
            etl_resource.setMain_serv_sync(Main_Server_Sync.YES.getCode());
            asmSql.addSql("select count(*) from etl_resource where etl_sys_id = ? and resource_type = ?");
            asmSql.addParam(etlSys.getEtl_sys_id());
            asmSql.addParam(etl_resource.getResource_type());
            if (SqlOperator.queryNumber(db, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("检查参数登记信息的SQL错误!"))) != 1) {
                etl_resource.add(db);
            }
        }
    }

    private static Map<String, Object> getFtpAgentPath(Long ftp_id, DatabaseWrapper db) {
        long countNum = SqlOperator.queryNumber(db, "SELECT COUNT(1) FROM " + FtpCollect.TableName + " WHERE ftp_id = ?", ftp_id).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            throw new BusinessException("当前任务(" + ftp_id + ")不再存在");
        }
        Map<String, Object> map = SqlOperator.queryOneObject(db, "SELECT t3.ai_desc pro_dic,t3.log_dir log_dic,t2.source_id FROM " + FtpCollect.TableName + " t1 JOIN " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id JOIN " + AgentDownInfo.TableName + " t3 ON t2.agent_ip = t3.agent_ip AND t2.agent_port = t3.agent_port " + " WHERE t1.ftp_id = ? LIMIT 1", ftp_id);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    public static List<EtlSys> getProInfo(DatabaseWrapper db, long user_id) {
        return SqlOperator.queryList(db, EtlSys.class, "select * from " + EtlSys.TableName + " where user_id=? order by etl_sys_cd", user_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<EtlSubSysList> getTaskInfo(DatabaseWrapper db, String etl_sys_cd) {
        return SqlOperator.queryList(db, EtlSubSysList.class, "select * from " + EtlSubSysList.TableName + " where" + " etl_sys_cd =? order by sub_sys_cd", etl_sys_cd);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "para_cd", desc = "", range = "")
    @Param(name = "pro_val", desc = "", range = "")
    public static void setDefaultEtlParaConf(DatabaseWrapper db, Long etl_sys_id, String para_cd, String pro_val) {
        long resourceNum = SqlOperator.queryNumber(db, "SELECT COUNT(1) FROM " + EtlPara.TableName + " WHERE etl_sys_id = ? AND para_cd = ?", etl_sys_id, para_cd).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (resourceNum == 0) {
            EtlPara etl_para = new EtlPara();
            etl_para.setEtl_sys_id(etl_sys_id);
            etl_para.setPara_cd(para_cd);
            etl_para.setPara_val(pro_val);
            etl_para.setPara_type(ParamType.LuJing.getCode());
            etl_para.add(db);
        } else {
            SqlOperator.execute(db, "update " + EtlPara.TableName + " set para_val = ? where etl_sys_id = ? AND para_cd = ?", pro_val, etl_sys_id, para_cd);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    public static void setDefaultEtlResource(DatabaseWrapper db, Long etl_sys_id) {
        long resourceNum = SqlOperator.queryNumber(db, "SELECT COUNT(1) FROM " + EtlResource.TableName + " WHERE resource_type = ? AND etl_sys_id = ?", Constant.NORMAL_DEFAULT_RESOURCE_TYPE, etl_sys_id).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (resourceNum == 0) {
            EtlResource etl_resource = new EtlResource();
            etl_resource.setEtl_sys_id(etl_sys_id);
            etl_resource.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
            etl_resource.setResource_max(Constant.RESOURCE_NUM);
            etl_resource.setMain_serv_sync(Main_Server_Sync.YES.getCode());
            etl_resource.add(db);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Long> getEtlJob(DatabaseWrapper db, Long etl_sys_id, Long sub_sys_id) {
        return SqlOperator.queryOneColumnList(db, "SELECT etl_job_id FROM " + EtlJobDef.TableName + " WHERE etl_sys_id = ? AND sub_sys_id = ?", etl_sys_id, sub_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    @Return(desc = "", range = "")
    public static EtlJobDef getEtlJobByJob(DatabaseWrapper db, Long etl_sys_id, String etl_job) {
        return SqlOperator.queryOneObject(db, EtlJobDef.class, "SELECT etl_job_id FROM " + EtlJobDef.TableName + " WHERE etl_sys_id = ? AND etl_job = ?", etl_sys_id, etl_job).orElseThrow(() -> new BusinessException("根据工程id与作业名称获取作业信息失败"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Long> getJobResource(DatabaseWrapper db, Long etl_sys_id) {
        return SqlOperator.queryOneColumnList(db, "SELECT etl_job_id FROM " + EtlJobResourceRela.TableName + " WHERE etl_sys_id = ? ", etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_def", desc = "", range = "", isBean = true)
    @Param(name = "jobResource", desc = "", range = "")
    public static void setEtl_job_resource_rela(DatabaseWrapper db, Long etl_sys_id, EtlJobDef etl_job_def, List<Long> jobResource) {
        Long etlJobId = etl_job_def.getEtl_job_id();
        if (!jobResource.contains(etlJobId)) {
            EtlJobResourceRela etl_job_resource_rela = new EtlJobResourceRela();
            etl_job_resource_rela.setEtl_sys_id(etl_sys_id);
            etl_job_resource_rela.setEtl_job_id(etlJobId);
            etl_job_resource_rela.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
            etl_job_resource_rela.setResource_req(Constant.JOB_RESOURCE_NUM);
            etl_job_resource_rela.add(db);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "pre_etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Result downEtlJobDependencyInfo(Long pre_etl_sys_id, Long etl_job_id, DatabaseWrapper db) {
        Result downJob = SqlOperator.queryResult(db, "select ejd.etl_job_id,ejd.etl_job,ejd.etl_sys_id,ed.pre_etl_job_id from " + EtlDependency.TableName + " ed left join " + EtlJobDef.TableName + " ejd on ed.etl_sys_id=ejd.etl_sys_id and ed.etl_job_id=ejd.etl_job_id " + " WHERE ed.pre_etl_sys_id=? and ed.pre_etl_job_id=? " + " order by ejd.job_priority DESC,ed.etl_sys_id,ed.etl_job_id", pre_etl_sys_id, etl_job_id);
        if (!downJob.isEmpty()) {
            for (int i = 0; i < downJob.getRowCount(); i++) {
                downJob.setObject(i, "id", downJob.getString(i, "etl_job_id"));
                downJob.setObject(i, "name", downJob.getString(i, "etl_job"));
                downJob.setObject(i, "direction", "right");
                downJob.setObject(i, "topic", downJob.getString(i, "etl_job"));
                downJob.setObject(i, "background-color", "#0000ff");
            }
        }
        return downJob;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Result topEtlJobDependencyInfo(Long etl_job_id, Long etl_sys_id, DatabaseWrapper db) {
        Result topJob = SqlOperator.queryResult(db, "select ejd.etl_sys_id,ed.pre_etl_job_id,ejd.etl_job as pre_etl_job" + " FROM " + EtlDependency.TableName + " ed join " + EtlJobDef.TableName + " ejd" + " ON ed.pre_etl_job_id = ejd.etl_job_id" + " WHERE ed.etl_job_id=? AND ed.etl_sys_id=?", etl_job_id, etl_sys_id);
        if (!topJob.isEmpty()) {
            for (int i = 0; i < topJob.getRowCount(); i++) {
                topJob.setObject(i, "id", topJob.getString(i, "pre_etl_job_id"));
                topJob.setObject(i, "name", topJob.getString(i, "pre_etl_job"));
                topJob.setObject(i, "direction", "left");
                topJob.setObject(i, "topic", topJob.getString(i, "pre_etl_job"));
                topJob.setObject(i, "background-color", "#0000ff");
            }
        }
        return topJob;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public static boolean isEtlSysExist(Long etl_sys_id, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "select count(*) from " + EtlSys.TableName + " where etl_sys_id=?", etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public static boolean isEtlSysExistById(Long etl_sys_id, Long user_id, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "select count(*) from " + EtlSys.TableName + " where etl_sys_id=? and user_id=?", etl_sys_id, user_id).orElseThrow(() -> new BusinessException("sql查询错误")) <= 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlSubSysExist(Long etl_sys_id, Long sub_sys_id, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "SELECT count(1) FROM " + EtlSubSysList.TableName + " WHERE etl_sys_id=? AND sub_sys_id=?", etl_sys_id, sub_sys_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 1;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "sub_sys_id", desc = "", range = "")
    public static void isEtlJobDefExistUnderEtlSubSys(Long etl_sys_id, Long sub_sys_id, DatabaseWrapper db) {
        if (SqlOperator.queryNumber(db, "select count(1) from " + EtlJobDef.TableName + "  WHERE etl_sys_id=? AND sub_sys_id=?", etl_sys_id, sub_sys_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("该工程对应的任务下还有作业，不能删除！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etlSys", desc = "", range = "")
    public static void isETLDeploy(EtlSys etlSys) {
        Validator.notBlank(etlSys.getEtl_serv_ip(), "服务器IP为空，请检查工程是否已部署！");
        Validator.notBlank(etlSys.getEtl_serv_port(), "服务器端口是否为空，请检查工程是否已部署！");
        Validator.notBlank(etlSys.getUser_name(), "服务器用户名为空，请检查工程是否已部署！");
        Validator.notBlank(etlSys.getUser_pwd(), "服务器密码为空，请检查工程是否已部署！");
        Validator.notBlank(etlSys.getServ_file_path(), "服务器部署目录为空，请检查工程是否已部署！");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlJobDefExist(Long etl_sys_id, Long etl_job_id, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "SELECT count(1) FROM " + EtlJobDef.TableName + " WHERE etl_job_id=? AND etl_sys_id=?", etl_job_id, etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlJobDefExist(Long etl_sys_id, String etl_job, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "SELECT count(1) FROM " + EtlJobDef.TableName + " WHERE etl_job=? AND etl_sys_id=?", etl_job, etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    public static boolean isEtlJObDefExistBySysCd(Long etl_sys_id, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "SELECT count(1) FROM " + EtlJobDef.TableName + " WHERE etl_sys_id=?", etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误")) != 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "resource_type", desc = "", range = "")
    @Param(name = "resource_seq", desc = "", range = "")
    public static void isResourceDemandTooLarge(Long etl_sys_id, String resource_type, Integer resource_seq, DatabaseWrapper db) {
        if (!isEtlResourceExist(etl_sys_id, resource_type, db)) {
            throw new BusinessException("当前工程对应的资源已不存在！");
        }
        List<Integer> resource_max = SqlOperator.queryOneColumnList(db, "select resource_max from " + EtlResource.TableName + " where etl_sys_id=? AND resource_type=?", etl_sys_id, resource_type);
        if (resource_seq > resource_max.get(0)) {
            throw new BusinessException("当前分配的作业资源需求数过大 ,已超过当前资源类型的最大阀值数!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlJobResourceRelaExist(Long etl_sys_id, Long etl_job_id, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "select count(*) from " + EtlJobResourceRela.TableName + " where etl_sys_id = ? and etl_job_id = ?", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "resource_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlJobResourceRelaExistByType(Long etl_sys_id, String resource_type, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "select count(*) from " + EtlJobResourceRela.TableName + " where etl_sys_id = ? and resource_type = ?", etl_sys_id, resource_type).orElseThrow(() -> new BusinessException("sql查询错误")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "resource_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlResourceExist(Long etl_sys_id, String resource_type, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "SELECT count(1) FROM " + EtlResource.TableName + " WHERE resource_type=? AND etl_sys_id=?", resource_type, etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "pre_etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Param(name = "pre_etl_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlDependencyExist(Long etl_sys_id, Long pre_etl_sys_id, Long etl_job_id, Long pre_etl_job_id, DatabaseWrapper db) {
        if (etl_job_id.equals(pre_etl_job_id)) {
            throw new BusinessException("作业名称与上游作业名称相同不能依赖！");
        }
        return SqlOperator.queryNumber(db, "select count(*) from " + EtlDependency.TableName + " where etl_sys_id=? And etl_job_id=? AND pre_etl_sys_id=? AND pre_etl_job_id=?", etl_sys_id, etl_job_id, pre_etl_sys_id, pre_etl_job_id).orElseThrow(() -> new BusinessException("sql查询错误")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "para_cd", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlParaExist(Long etl_sys_id, String para_cd, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "select count(*) from " + EtlPara.TableName + " where etl_sys_id=? AND para_cd=?", etl_sys_id, para_cd).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean isEtlJobHandExist(Long etl_sys_id, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "select count(*) from " + EtlJobHand.TableName + " where etl_sys_id=?", etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public static boolean isEtlJobHandExistByJob(Long etl_sys_id, Long etl_job_id, DatabaseWrapper db) {
        return SqlOperator.queryNumber(db, "select count(*) from " + EtlJobHand.TableName + " where etl_sys_id=? and etl_job_id=?", etl_sys_id, etl_job_id).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static EtlSys getEtlSysById(Long etl_sys_id, long user_id, DatabaseWrapper db) {
        return SqlOperator.queryOneObject(db, EtlSys.class, "select * from " + EtlSys.TableName + " where user_id=? and etl_sys_id=?", user_id, etl_sys_id).orElseThrow(() -> new BusinessException("sql查询错误或映射实体失败"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Param(name = "etl_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getEtlJobByJob(Long etl_sys_id, Long etl_job_id, DatabaseWrapper db) {
        return SqlOperator.queryOneObject(db, "select * FROM " + EtlJobDef.TableName + " where etl_sys_id=? AND etl_job_id=?", etl_sys_id, etl_job_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "compressCommand", desc = "", range = "")
    @Param(name = "etlSysInfo", desc = "", range = "")
    @Param(name = "sshDetails", desc = "", range = "")
    public static void interactingWithTheAgentServer(String compressCommand, EtlSys etlSysInfo, SSHDetails sshDetails) throws IOException {
        sshDetails.setHost(etlSysInfo.getEtl_serv_ip());
        sshDetails.setPort(Integer.parseInt(etlSysInfo.getEtl_serv_port()));
        sshDetails.setUser_name(etlSysInfo.getUser_name());
        sshDetails.setPwd(etlSysInfo.getUser_pwd());
        try (SSHOperate sshOperate = new SSHOperate(sshDetails, 0)) {
            sshOperate.execCommandBySSH(compressCommand);
        } catch (JSchException e) {
            throw new SystemBusinessException("命令: '" + compressCommand + "' 执行失败!" + e);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String getEtlJobChildId(Long etl_sys_id, DatabaseWrapper db) {
        List<String> jobChildIds = SqlOperator.queryOneColumnList(db, "select job_child_id FROM " + EtlJobCpid.TableName + " where etl_sys_id = ?", etl_sys_id);
        if (!jobChildIds.isEmpty()) {
            return StringUtil.join(jobChildIds, ",");
        }
        return "";
    }
}
