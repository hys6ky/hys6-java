package hyren.serv6.b.batchcollection.unstructuredAgent;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.packutil.PackUtil;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.DboExecute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/9/11 14:47")
public class UnstructuredFileCollectService {

    @Method(desc = "", logicStep = "")
    @Param(name = "file_collect_set", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public Map<String, Object> searchFileCollect(FileCollectSet file_collect_set) {
        if (file_collect_set.getAgent_id() == null) {
            throw new BusinessException("agent_id不能为空");
        }
        String url = AgentActionUtil.getUrl(file_collect_set.getAgent_id(), UserUtil.getUserId(), AgentActionUtil.GETSERVERINFO);
        try {
            HttpClient.ResponseValue resVal = new HttpClient().get(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("连接" + url + "失败");
            }
            Map map = (Map<String, Object>) ar.getData();
            map.put("localdate", DateUtil.getSysDate());
            map.put("localtime", DateUtil.getSysTime());
            if (file_collect_set.getFcs_id() != null) {
                FileCollectSet file_collect_set_info = Dbo.queryOneObject(FileCollectSet.class, "SELECT * FROM " + FileCollectSet.TableName + " WHERE fcs_id = ?", file_collect_set.getFcs_id()).orElseThrow(() -> new BusinessException("根据fcs_id" + file_collect_set.getFcs_id() + "查询不到file_collect_set表信息"));
                map.put("file_collect_set_info", file_collect_set_info);
            }
            String sqlStr = " SELECT fs.fcs_id id,fs.fcs_name task_name,fs.is_solr is_solr,fs.AGENT_ID AGENT_ID,gi.source_id,gi.agent_type" + " FROM " + FileCollectSet.TableName + " fs " + " LEFT JOIN " + AgentInfo.TableName + " gi ON gi.Agent_id = fs.Agent_id " + " where fs.Agent_id = ? AND fs.is_sendok = ?";
            List<Map<String, Object>> taskInfoList = Dbo.queryList(Dbo.db(), sqlStr, file_collect_set.getAgent_id(), IsFlag.Fou.getCode());
            Map<String, Object> taskInfo = new HashMap<String, Object>();
            if (taskInfoList.size() == 1 && !taskInfoList.isEmpty()) {
                taskInfo = taskInfoList.get(0);
            }
            map.put("taskInfo", taskInfo);
            return map;
        } catch (Exception e) {
            throw new BusinessException("与Agent端服务交互异常!!!" + e.getMessage());
        }
    }

    public Map<String, Object> searchFileCollectByAgent(Long agentId) {
        String sqlStr = " SELECT fs.fcs_id id,fs.fcs_name task_name,fs.is_solr is_solr,fs.AGENT_ID AGENT_ID,gi.source_id,gi.agent_type" + " FROM " + FileCollectSet.TableName + " fs " + " LEFT JOIN " + AgentInfo.TableName + " gi ON gi.Agent_id = fs.Agent_id " + " where fs.Agent_id = ? AND fs.is_sendok = ?";
        List<Map<String, Object>> taskInfoList = Dbo.queryList(Dbo.db(), sqlStr, agentId, IsFlag.Fou.getCode());
        if (taskInfoList.size() == 1 && !taskInfoList.isEmpty()) {
            Map<String, Object> taskInfo = taskInfoList.get(0);
            return taskInfo;
        }
        return null;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_collect_set", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public long addFileCollect(FileCollectSet file_collect_set) {
        long count = Dbo.queryNumber("SELECT count(1) count FROM " + FileCollectSet.TableName + " WHERE fcs_name = ?", file_collect_set.getFcs_name()).orElseThrow(() -> new BusinessException("查询得到的数据必须有且只有一条"));
        if (count > 0) {
            throw new BusinessException("非结构化任务名称重复");
        }
        file_collect_set.setFcs_id(PrimayKeyGener.getNextId());
        file_collect_set.setIs_sendok(IsFlag.Fou.getCode());
        file_collect_set.add(Dbo.db());
        return file_collect_set.getFcs_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_collect_set", desc = "", range = "", isBean = true)
    public void updateFileCollect(FileCollectSet file_collect_set) {
        if (file_collect_set.getFcs_id() == null) {
            throw new BusinessException("更新file_collect_set表时fcs_id不能为空");
        }
        FileCollectSet fcs_info = Dbo.queryOneObject(FileCollectSet.class, "SELECT * FROM " + FileCollectSet.TableName + " WHERE fcs_id=?", file_collect_set.getFcs_id()).orElseThrow(() -> (new BusinessException("查询文件采集任务信息的SQL失败!")));
        if (!fcs_info.getFcs_name().equals(file_collect_set.getFcs_name())) {
            long count = Dbo.queryNumber("SELECT count(1) count FROM " + FileCollectSet.TableName + " WHERE " + "fcs_name = ? AND fcs_id != ?", file_collect_set.getFcs_name(), file_collect_set.getFcs_id()).orElseThrow(() -> new BusinessException("查询得到的数据必须有且只有一条"));
            if (count > 0) {
                throw new BusinessException("非结构化任务名称重复");
            }
        }
        file_collect_set.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fcs_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result searchFileSource(long fcs_id) {
        return Dbo.queryResult("SELECT * FROM " + FileSource.TableName + " WHERE fcs_id = ?", fcs_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Param(name = "path", desc = "", valueIfNull = "", range = "")
    @Return(desc = "", range = "")
    public List<Map> selectPath(long agent_id, String path) {
        String url = AgentActionUtil.getUrl(agent_id, UserUtil.getUserId(), AgentActionUtil.GETSYSTEMFILEINFO);
        try {
            HttpClient.ResponseValue resVal = new HttpClient().addData("pathVal", path).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("连接远程Agent获取文件夹失败");
            }
            List<Map> list = new ArrayList<>();
            list.addAll((Collection<? extends Map>) ar.getData());
            return list;
        } catch (Exception e) {
            throw new BusinessException("与Agent端服务交互异常" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_sources_array", desc = "", range = "")
    public void saveFileSource(List<FileSource> file_sources) {
        Long fcs_id;
        if (file_sources != null && file_sources.size() > 0) {
            fcs_id = file_sources.get(0).getFcs_id();
            if (fcs_id == null) {
                throw new BusinessException("fcs_id不能为空");
            }
        } else {
            throw new BusinessException("源文件设置信息有且最少有一个");
        }
        int delete_num = Dbo.execute("DELETE FROM  " + FileSource.TableName + " WHERE fcs_id = ?", fcs_id);
        if (delete_num < 0) {
            throw new BusinessException("根据fcs_id = " + fcs_id + "删除源文件设置表失败");
        }
        for (FileSource file_source : file_sources) {
            int lastIndex = file_source.getFile_source_path().length() - 1;
            if (file_source.getFile_source_path().lastIndexOf("\\") == lastIndex) {
                String substring = file_source.getFile_source_path().substring(0, lastIndex);
                file_source.setFile_source_path(substring);
            }
            long count = Dbo.queryNumber("SELECT count(1) count FROM " + FileSource.TableName + " WHERE fcs_id = ? AND file_source_path = ?", fcs_id, file_source.getFile_source_path()).orElseThrow(() -> new BusinessException("查询得到的数据必须有且只有一条"));
            if (count > 0) {
                throw new BusinessException("同一个非结构化采集请不要选择重复的文件路径");
            }
            file_source.setFile_source_id(PrimayKeyGener.getNextId());
            file_source.add(Dbo.db());
        }
        DboExecute.updatesOrThrow("更新表" + FileCollectSet.TableName + "失败", "UPDATE " + FileCollectSet.TableName + " SET is_sendok = ?" + " WHERE fcs_id = ? ", IsFlag.Shi.getCode(), fcs_id);
        executeJob(fcs_id, "execute_etl");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fcs_id", desc = "", range = "")
    @Param(name = "execute_type", desc = "", range = "")
    public void executeJob(long fcs_id, String execute_type) {
        Result result = Dbo.queryResult("SELECT t1.*,t2.agent_name,t3.source_id,t3.datasource_name,t4.dep_id " + " FROM " + FileCollectSet.TableName + " t1 " + " LEFT JOIN " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id " + " LEFT JOIN " + DataSource.TableName + " t3 ON t3.source_id = t2.source_id " + " LEFT JOIN " + SysUser.TableName + " t4 ON t4.user_id = t2.user_id " + " WHERE t1.fcs_id = ?", fcs_id);
        if (result.isEmpty()) {
            throw new BusinessException("查询不到数据，请检查传入的id是否正确");
        }
        List<Map<String, Object>> jsonArray = JsonUtil.toObject(result.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        Map<String, Object> object = jsonArray.get(0);
        Result source = Dbo.queryResult("SELECT * FROM " + FileSource.TableName + " where fcs_id = ?", fcs_id);
        if (source.isEmpty()) {
            throw new BusinessException("查询不到文件源设置表数据，请检查传入的id是否正确");
        }
        List<Map<String, Object>> data = JsonUtil.toObject(source.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        object.put("file_sourceList", data);
        long agent_id = result.getLong(0, "agent_id");
        String url;
        if ("execute_etl".equals(execute_type)) {
            url = AgentActionUtil.getUrl(agent_id, UserUtil.getUserId(), AgentActionUtil.EXECUTEFILECOLLECT);
        } else if ("execute_immediately".equals(execute_type)) {
            url = AgentActionUtil.getUrl(agent_id, UserUtil.getUserId(), AgentActionUtil.EXECUTEFILECOLLECTIMMEDIATELY);
        } else {
            throw new BusinessException("执行类型不合法! execute_type: " + execute_type + "see(execute_immediately:立即执行,execute_etl:作业调度执行)");
        }
        try {
            HttpClient.ResponseValue resVal = new HttpClient().addData("fileCollectTaskInfo", PackUtil.packMsg(JsonUtil.toJson(object))).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("执行文件采集作业失败");
            }
        } catch (Exception e) {
            throw new BusinessException("与Agent端服务交互异常!!!" + e.getMessage());
        }
    }
}
