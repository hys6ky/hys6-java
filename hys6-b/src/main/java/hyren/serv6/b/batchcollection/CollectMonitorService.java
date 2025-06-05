package hyren.serv6.b.batchcollection;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.ExecuteState;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@Slf4j
@DocClass(desc = "", author = "Mr.Lee", createdate = "2019-09-04 12:09")
public class CollectMonitorService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAgentNumAndSourceNum() {
        return Dbo.queryOneObject("SELECT COUNT(agent_id) agentNum,COUNT(DISTINCT(source_id)) sourceNum FROM " + AgentInfo.TableName + " WHERE user_id = ?", UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getDatabaseSet() {
        return Dbo.queryList("SELECT task_name taskname ,database_id taskid,task.Agent_id,(case agent_type when ? then ? else ? end) agent_type FROM " + DatabaseSet.TableName + " task JOIN " + AgentInfo.TableName + " ai ON task.agent_id = ai.agent_id WHERE user_id = ? and " + "task.is_sendok = ? and agent_type in (?,?) ORDER BY taskid DESC ", AgentType.ShuJuKu.getCode(), AgentType.ShuJuKu.getValue(), AgentType.DBWenJian.getValue(), UserUtil.getUserId(), IsFlag.Shi.getCode(), AgentType.ShuJuKu.getCode(), AgentType.DBWenJian.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getDataCollectInfo() {
        Result result = Dbo.queryResult(" select sum((CASE WHEN collect_type = ? THEN file_size ELSE 0 END)) filecollectsize," + "sum((CASE WHEN collect_type in (?,?) THEN file_size ELSE 0 END)) dbcollectsize FROM " + DataStoreReg.TableName + " sfa JOIN  " + AgentInfo.TableName + " ai ON sfa.agent_id = ai.agent_id WHERE user_id = ?", AgentType.WenJianXiTong.getCode(), AgentType.ShuJuKu.getCode(), AgentType.DBWenJian.getCode(), UserUtil.getUserId());
        result.setObject(0, "filesize", FileUtil.fileSizeConversion(result.getLongDefaultZero(0, "filecollectsize")));
        result.setObject(0, "dbsize", FileUtil.fileSizeConversion(result.getLongDefaultZero(0, "dbcollectsize")));
        result.setObject(0, "taskNum", Dbo.queryNumber("SELECT COUNT( 1 ) taskNum FROM ( SELECT database_id id, agent_id, is_sendok FROM  " + DatabaseSet.TableName + " UNION ALL SELECT fcs_id id, agent_id, is_sendok FROM file_collect_set ) A" + " WHERE EXISTS ( SELECT 1 FROM " + AgentInfo.TableName + " ai WHERE ai.user_id = ? " + " AND ai.agent_id = A.Agent_id ) AND is_sendok = ?", UserUtil.getUserId(), IsFlag.Shi.getCode()).orElseThrow(() -> new BusinessException("未获取到采集任务数量")));
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getCurrentTaskJob(long database_id) {
        List<CollectCase> collectJobList = Dbo.queryList(CollectCase.class, "SELECT task_classify,collect_type,job_type,execute_state,collect_s_date,collect_s_time," + "collect_e_date,collect_e_time,cc_remark FROM " + CollectCase.TableName + " WHERE collect_set_id = ?" + " AND etl_date = (select max(etl_date) FROM collect_case WHERE " + "collect_set_id = ? ) ORDER BY task_classify", database_id, database_id);
        Map<String, Object> collectMap = new HashMap<>();
        collectMap.put("collectTableData", JobTableDetails.getTableDetails(collectJobList));
        collectMap.put("failure", ExecuteState.YunXingShiBai.getCode());
        collectMap.put("success", ExecuteState.YunXingWanCheng.getCode());
        collectMap.put("running", ExecuteState.KaiShiYunXing.getCode());
        return collectMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Map<String, String>> getHoStoryCollect() {
        Result result = Dbo.queryResult("SELECT * FROM (SELECT SUM(CAST(cf.collet_database_size AS NUMERIC)) dbsize,cf.etl_date AS dbdate FROM " + AgentInfo.TableName + " ai JOIN " + CollectCase.TableName + " cf ON cf.agent_id = ai.agent_id WHERE " + " ai.user_id = ? AND cf.collect_type IN ( ?, ? ) GROUP BY cf.etl_date ORDER BY cf.etl_date DESC) aa " + "FULL JOIN (SELECT SUM( file_size ) filesize,cc.etl_date AS filedate FROM ( SELECT * FROM collect_case " + "WHERE collect_type = ? ) cc JOIN ( SELECT * FROM " + SourceFileAttribute.TableName + " WHERE " + "collect_type = ? ) sfa ON cc.agent_id = sfa.agent_id AND cc.collect_set_id = sfa.collect_set_id JOIN " + AgentInfo.TableName + " ai ON cc.agent_id = ai.agent_id WHERE ai.user_id = ? GROUP BY" + " cc.etl_date ORDER BY cc.etl_date DESC) bb ON aa.dbdate = bb.filedate LIMIT 15", UserUtil.getUserId(), AgentType.DBWenJian.getCode(), AgentType.ShuJuKu.getCode(), AgentType.WenJianXiTong.getCode(), AgentType.WenJianXiTong.getCode(), UserUtil.getUserId());
        if (result.isEmpty()) {
            return null;
        }
        List<Map<String, String>> resultMap = new ArrayList<Map<String, String>>();
        Map<String, String> itemMap = null;
        for (int i = 0; i < result.getRowCount(); i++) {
            itemMap = new LinkedHashMap<>();
            itemMap.put("date", DateUtil.parseStr2DateWith8Char(result.getString(i, "dbdate")).toString());
            itemMap.put("data", formatFileSizeMB(result.getLongDefaultZero(i, "dbsize")));
            itemMap.put("file", formatFileSizeMB(result.getLongDefaultZero(i, "filesize")));
            resultMap.add(itemMap);
        }
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "size", desc = "", range = "")
    @Return(desc = "", range = "")
    private String formatFileSizeMB(long size) {
        double f = (double) size / (1024 * 1024);
        return String.format("%.2f", f);
    }
}
