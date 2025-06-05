package hyren.serv6.b.batchcollection.semiStructuredAgent.startmodeconf;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.tools.CommonUtils;
import hyren.serv6.base.codes.Dispatch_Frequency;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Pro_Type;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.jobUtil.EtlJobUtil;
import hyren.serv6.commons.jobUtil.beans.JobStartConf;
import hyren.serv6.commons.jobUtil.beans.ObjJobBean;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.ApiImplicitParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
@DocClass(desc = "", author = "dhw", createdate = "2020/6/16 11:13")
public class StartModeConfService {

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getEtlJobConfInfoFromObj(long odc_id) {
        List<Map<String, Object>> etlRelationJobList = Dbo.queryList("SELECT t1.take_id,t2.*,es.etl_sys_cd,ess.sub_sys_cd FROM " + TakeRelationEtl.TableName + " t1" + " JOIN " + EtlJobDef.TableName + " t2 ON t1.etl_job_id = t2.etl_job_id" + " JOIN " + EtlSys.TableName + " es ON t1.etl_sys_id = es.etl_sys_id" + " JOIN " + EtlSubSysList.TableName + " ess ON ess.sub_sys_id = t1.sub_sys_id" + " JOIN " + EtlJobDef.TableName + " ejd ON ejd.etl_job_id = t1.etl_job_id" + " WHERE t1.etl_sys_id = t2.etl_sys_id AND t1.sub_sys_id = t2.sub_sys_id AND t1.take_id = ? ", odc_id);
        setPreJobList(etlRelationJobList);
        List<Map<String, Object>> previewJobList = getPreviewJob(odc_id);
        List<Object> defaultEtlJob = previewJobList.stream().map(item -> item.get("etl_job")).collect(Collectors.toList());
        List<Object> etlRelationJobData = etlRelationJobList.stream().map(item -> item.get("etl_job")).collect(Collectors.toList());
        List<Object> reduceDeleteList = etlRelationJobData.stream().filter(item -> !defaultEtlJob.contains(item)).collect(Collectors.toList());
        etlRelationJobList.removeIf(itemMap -> {
            if (reduceDeleteList.contains(itemMap.get("etl_job"))) {
                Dbo.execute("DELETE FROM " + TakeRelationEtl.TableName + " WHERE etl_job_id = ?", itemMap.get("etl_job_id"));
                Dbo.execute("DELETE FROM " + EtlJobDef.TableName + " WHERE etl_job_id = ? AND etl_sys_id = ? AND sub_sys_id = ?", itemMap.get("etl_job_id"), itemMap.get("etl_sys_id"), itemMap.get("sub_sys_id"));
                return true;
            } else {
                return false;
            }
        });
        List<Object> reduceAddList = defaultEtlJob.stream().filter(item -> !etlRelationJobData.contains(item)).collect(Collectors.toList());
        previewJobList.removeIf(item -> !reduceAddList.contains(item.get("etl_job")));
        etlRelationJobList.addAll(previewJobList);
        return etlRelationJobList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etlJobList", desc = "", range = "")
    private void setPreJobList(List<Map<String, Object>> etlJobList) {
        etlJobList.forEach(itemMap -> {
            List<Object> preJobList = Dbo.queryOneColumnList("SELECT pre_etl_job_id FROM " + EtlDependency.TableName + " WHERE etl_sys_id = ? AND etl_job_id = ?", Long.parseLong(itemMap.get("etl_sys_id").toString()), Long.parseLong(itemMap.get("etl_job_id").toString()));
            itemMap.put("pre_etl_job_id", preJobList);
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getPreviewJob(long odc_id) {
        CommonUtils.isObjectCollectExist(odc_id);
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + ObjectCollectTask.TableName + " WHERE odc_id = ?", odc_id).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum < 1) {
            throw new BusinessException("当前任务(" + odc_id + ")下不存在表信息");
        }
        List<Map<String, Object>> tableList = Dbo.queryList("select oct.ocs_id,oct.en_name,oct.zh_name,ai.agent_type,ds.datasource_number," + "ds.datasource_name,oc.obj_number,oc.obj_collect_name,ai.agent_id,ai.agent_name" + " FROM " + ObjectCollectTask.TableName + " oct" + " JOIN " + ObjectCollect.TableName + " oc ON oc.odc_id = oct.odc_id" + " JOIN " + AgentInfo.TableName + " ai ON oc.agent_id = ai.agent_id" + " JOIN " + DataSource.TableName + " ds ON ai.source_id=ds.source_id " + " WHERE oct.odc_id = ? ORDER BY oct.en_name", odc_id);
        tableList.forEach(itemMap -> setObjCollectJobParam(odc_id, itemMap));
        return tableList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "tableItemMap", desc = "", range = "")
    private void setObjCollectJobParam(long odc_id, Map<String, Object> tableItemMap) {
        String pro_name = tableItemMap.get("datasource_number") + Constant.SPLITTER + tableItemMap.get("obj_number") + Constant.SPLITTER + tableItemMap.get("en_name") + Constant.SPLITTER + tableItemMap.get("agent_id");
        tableItemMap.put("etl_job", pro_name);
        String etl_job_desc = tableItemMap.get("datasource_name") + Constant.SPLITTER + tableItemMap.get("obj_collect_name") + Constant.SPLITTER + tableItemMap.get("zh_name") + Constant.SPLITTER + tableItemMap.get("agent_name");
        tableItemMap.put("etl_job_desc", etl_job_desc);
        String pro_para = odc_id + Constant.ETLPARASEPARATOR + Constant.BATCH_DATE;
        tableItemMap.put("pro_para", pro_para);
        tableItemMap.put("disp_freq", Dispatch_Frequency.DAILY.getCode());
        tableItemMap.put("job_priority", 0);
        tableItemMap.put("disp_offset", 0);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAgentPath(long odc_id) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + ObjectCollect.TableName + " WHERE odc_id = ?", odc_id).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            throw new BusinessException("当前任务(" + odc_id + ")不再存在");
        }
        Map<String, Object> map = Dbo.queryOneObject("SELECT distinct t3.ai_desc pro_dic,t3.log_dir log_dic" + " FROM " + ObjectCollect.TableName + " t1" + " JOIN " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id" + " JOIN " + AgentDownInfo.TableName + " t3 ON t2.agent_ip = t3.agent_ip AND t2.agent_port = t3.agent_port" + " WHERE t1.odc_id = ?", odc_id);
        map.put("pro_type", Pro_Type.SHELL.getCode());
        map.put("pro_name", Constant.SEMISTRUCTURED_JOB_COMMAND);
        Map<String, Object> objRelationEtlMap = Dbo.queryOneObject("SELECT es.etl_sys_cd, ess.sub_sys_cd, tre.* FROM " + TakeRelationEtl.TableName + " tre" + " JOIN " + EtlSys.TableName + " es ON tre.etl_sys_id = es.etl_sys_id" + " JOIN " + EtlSubSysList.TableName + " ess ON ess.sub_sys_id = tre.sub_sys_id" + " JOIN " + EtlJobDef.TableName + " ejd ON ejd.etl_job_id = tre.etl_job_id" + " WHERE take_id = ? LIMIT 1", odc_id);
        map.putAll(objRelationEtlMap);
        return map;
    }

    @Method(desc = "", logicStep = "")
    @ApiImplicitParam(name = "objJobBean", value = "", dataTypeClass = JobStartConf[].class)
    public void saveStartModeConfData(ObjJobBean objJobBean) {
        Long odc_id = objJobBean.getOdc_id();
        Validator.notNull(odc_id, "对象任务id不能为空");
        CommonUtils.isObjectCollectExist(odc_id);
        EtlJobUtil.saveObjJob(objJobBean, Dbo.db());
        DboExecute.updatesOrThrow("此次采集任务配置完成,更新发送状态失败", "UPDATE " + ObjectCollect.TableName + " SET is_sendok = ? WHERE odc_id = ?", IsFlag.Shi.getCode(), odc_id);
    }
}
