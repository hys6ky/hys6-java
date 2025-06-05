package hyren.serv6.h.market;

import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.h.market.dmjobtable.DmJobTableInfoDto;
import hyren.serv6.h.market.dmjobtable.DmJobTableInfoService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Data
@Component
public class HyrenProcessJobGenerator {

    private static final long serialVersionUID = 686808859541352385L;

    private Long etl_sys_id;

    private Long sub_sys_id;

    private DmModuleTable dmModuleTable;

    private List<String> etl_job_s;

    private List<Long> etl_job_id_resources;

    private static final String init_ = "init_";

    protected final String clear_ = "clear_";

    private List<Long> cur_job_list = new ArrayList<>();

    private String etl_date;

    private DatabaseWrapper db;

    private static final String _PRO_MAIN_NAME = "process-job-command.sh";

    private static final String _PRO_NAME_DIR = PropertyParaValue.getString("process.deployment.dir", "/data/HRSDATA/process_deployment_dir") + File.separator;

    private static final String _ETL_JOB_LOG_DIR_PREFIX_ = _PRO_NAME_DIR + "processlogs" + File.separator;

    Map<String, String> jobMap = new HashMap<>();

    public void generateJob() throws BusinessException {
        cur_job_list.clear();
        this.etl_job_s = SqlOperator.queryOneColumnList(this.db, "SELECT ETL_JOB FROM " + EtlJobDef.TableName + " WHERE etl_sys_id = ?", etl_sys_id);
        this.etl_job_id_resources = SqlOperator.queryOneColumnList(this.db, "SELECT etl_job_id FROM " + EtlJobResourceRela.TableName + " WHERE etl_sys_id = ?", etl_sys_id);
        String processTableName = dmModuleTable.getModule_table_en_name();
        try {
            List<DmJobTableInfoDto> dmJobTableInfoDtos = Dbo.queryList(DmJobTableInfoDto.class, "select dti.task_name,dti.task_number, djti.* from " + DmTaskInfo.TableName + " dti " + " join " + DmJobTableInfo.TableName + " djti on dti.task_id = djti.task_id" + " where dti.module_table_id = ?", dmModuleTable.getModule_table_id());
            for (DmJobTableInfoDto dmJobTableInfoDto : dmJobTableInfoDtos) {
                EtlJobDef etlJobDef = new EtlJobDef();
                Long jobId = PrimayKeyGener.getNextId();
                etlJobDef.setEtl_job_id(jobId);
                initEtlJobDef(etlJobDef);
                String etl_job = init_ + dmJobTableInfoDto.getTask_number() + '_' + dmJobTableInfoDto.getJobtab_en_name() + '_' + dmJobTableInfoDto.getJobtab_step_number();
                checkEtlJobIsExist(etl_job);
                etlJobDef.setEtl_job(etl_job);
                etlJobDef.setPro_name(_PRO_MAIN_NAME);
                String params = dmJobTableInfoDto.getModule_table_id() + Constant.ETLPARASEPARATOR + dmJobTableInfoDto.getJobtab_id() + Constant.ETLPARASEPARATOR + etl_date;
                etlJobDef.setPro_para(params);
                etlJobDef.setEtl_job_desc(String.format("模型表: %s, 作业: %s, 对应表名: %s, 对应表名的操作信息ID: %s", dmJobTableInfoDto.getJobtab_en_name(), dmJobTableInfoDto.getTask_name(), dmJobTableInfoDto.getJobtab_en_name(), dmJobTableInfoDto.getJobtab_id()));
                if (dmJobTableInfoDto.getJobtab_is_temp().equals(IsFlag.Shi.getCode())) {
                    etlJobDef.setEtl_job_desc(String.format("临时表: %s, 作业: %s, 对应表名: %s, 对应表名的操作信息ID: %s", dmJobTableInfoDto.getJobtab_en_name(), dmJobTableInfoDto.getTask_name(), dmJobTableInfoDto.getJobtab_en_name(), dmJobTableInfoDto.getJobtab_id()));
                }
                try {
                    jobMap.put(dmJobTableInfoDto.getJobtab_id().toString(), jobId.toString());
                    cur_job_list.add(etlJobDef.getEtl_job_id());
                    etlJobDef.add(db);
                } catch (Exception e) {
                    throw new BusinessException("保存作业 [ " + etlJobDef.getEtl_job() + " ] 失败! e: " + e);
                }
            }
            log.info("当前作业映射关系 : " + JsonUtil.toJson(jobMap));
            addDependency();
            log.info("作业依赖添加结束。");
            cur_job_list.forEach(etl_job_id -> {
                if (!checkJobResourcesIsExist(etl_job_id)) {
                    EtlJobResourceRela resource_rela = new EtlJobResourceRela();
                    resource_rela.setEtl_sys_id(etl_sys_id);
                    resource_rela.setEtl_job_id(etl_job_id);
                    resource_rela.setResource_type(Constant.NORMAL_DEFAULT_RESOURCE_TYPE);
                    resource_rela.setResource_req(1);
                    resource_rela.add(db);
                }
            });
            log.debug("当前作业列表信息 : " + JsonUtil.toJson(cur_job_list));
        } catch (Exception e) {
            throw new BusinessException("生成模型表 [ " + processTableName + " ] 的加工作业失败! e: " + e);
        }
    }

    @Autowired
    DmJobTableInfoService dmJobTableInfoService;

    private void addDependency() {
        List<DmJobTableInfo> jobsByModuleTableId = dmJobTableInfoService.findJobsByModuleTableId(dmModuleTable.getModule_table_id());
        List<String> tableNames = jobsByModuleTableId.stream().map(DmJobTableInfo::getJobtab_en_name).collect(Collectors.toList());
        List<Map<String, Object>> mapList = Dbo.queryList(" SELECT dd.jobtab_id, dd.jobtab_en_name, dds.own_source_table_name FROM " + DmJobTableInfo.TableName + " dd JOIN " + DmDatatableSource.TableName + " dds ON dd.jobtab_id = dds.jobtab_id");
        for (Map<String, Object> map : mapList) {
            if (tableNames.contains(map.get("own_source_table_name").toString())) {
                EtlJobDef etlJobDef = Dbo.queryOneObject(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where etl_job_id = ?", Long.parseLong(jobMap.get(map.get("jobtab_id").toString()))).orElseThrow(() -> new BusinessException(" find etl_job by jobtab_id is null"));
                List<DmJobTableInfo> dmJobTableInfos = Dbo.queryList(DmJobTableInfo.class, " select * from " + DmJobTableInfo.TableName + " where jobtab_en_name = ? and module_table_id = ?", map.get("own_source_table_name").toString(), dmModuleTable.getModule_table_id());
                for (DmJobTableInfo dmJobTableInfo : dmJobTableInfos) {
                    EtlJobDef preEtlJobDef = Dbo.queryOneObject(EtlJobDef.class, "select * from " + EtlJobDef.TableName + " where etl_job_id = ?", Long.parseLong(jobMap.get(dmJobTableInfo.getJobtab_id().toString()))).orElseThrow(() -> new BusinessException(" find etl_job by jobtab_id is null"));
                    addOneDepend(etlJobDef, preEtlJobDef);
                }
            }
        }
    }

    private void addOneDepend(EtlJobDef etlJobDef, EtlJobDef preEtlJobDef) {
        EtlDependency init_job_etl_dependency = new EtlDependency();
        init_job_etl_dependency.setEtl_sys_id(etlJobDef.getEtl_sys_id());
        init_job_etl_dependency.setEtl_job_id(etlJobDef.getEtl_job_id());
        init_job_etl_dependency.setPre_etl_sys_id(preEtlJobDef.getEtl_sys_id());
        init_job_etl_dependency.setPre_etl_job_id(preEtlJobDef.getEtl_job_id());
        init_job_etl_dependency.setStatus(Status.TRUE.getCode());
        Optional<EtlDependency> etlDependency = Dbo.queryOneObject(EtlDependency.class, " select * from " + EtlDependency.TableName + " where etl_sys_id = ? and etl_job_id = ? and pre_etl_sys_id = ? and pre_etl_job_id = ? " + " and status = ? ", init_job_etl_dependency.getEtl_sys_id(), init_job_etl_dependency.getEtl_job_id(), init_job_etl_dependency.getPre_etl_sys_id(), init_job_etl_dependency.getPre_etl_job_id(), init_job_etl_dependency.getStatus());
        if (etlDependency.isPresent()) {
            init_job_etl_dependency.update(db);
        } else {
            init_job_etl_dependency.add(db);
        }
    }

    private void checkEtlJobIsExist(String etl_job) {
        if (etl_job_s.contains(etl_job)) {
            EtlJobDef etlJobDef = Dbo.queryOneObject(EtlJobDef.class, " select * from " + EtlJobDef.TableName + " where etl_job = ?", etl_job).orElseThrow(() -> new BusinessException("sql failed.."));
            Dbo.execute(" delete from " + EtlJobDef.TableName + " where etl_job = ?", etl_job);
            Dbo.execute(" delete from " + EtlDependency.TableName + " where etl_job_id = ?", etlJobDef.getEtl_job_id());
            Dbo.execute(" delete from " + EtlJobResourceRela.TableName + " where etl_job_id = ?", etlJobDef.getEtl_job_id());
        }
    }

    private boolean checkJobResourcesIsExist(Long etl_job_id) {
        return etl_job_id_resources.contains(etl_job_id);
    }

    private void initEtlJobDef(EtlJobDef etl_job_def) {
        try {
            etl_job_def.setEtl_sys_id(etl_sys_id);
            etl_job_def.setSub_sys_id(sub_sys_id);
            etl_job_def.setPro_type(Pro_Type.SHELL.getCode());
            etl_job_def.setPro_dic(_PRO_NAME_DIR);
            etl_job_def.setLog_dic(_ETL_JOB_LOG_DIR_PREFIX_ + etl_sys_id + File.separator + sub_sys_id);
            etl_job_def.setDisp_freq(Dispatch_Frequency.DAILY.getCode());
            etl_job_def.setDisp_time("00:00:00");
            etl_job_def.setJob_eff_flag(Job_Effective_Flag.YES.getCode());
            etl_job_def.setToday_disp(Today_Dispatch_Flag.YES.getCode());
            etl_job_def.setJob_datasource(ETLDataSource.ShuJuJiaGong.getCode());
            etl_job_def.setDisp_offset(0);
            etl_job_def.setJob_priority(0);
            etl_job_def.setDisp_type(Dispatch_Type.TPLUS0.getCode());
            etl_job_def.setUpd_time(DateUtil.parseStr2DateWith8Char(DateUtil.getSysDate()) + " " + DateUtil.parseStr2TimeWith6Char(DateUtil.getSysTime()));
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("初始化作业定义信息失败! e: " + e);
        }
    }
}
