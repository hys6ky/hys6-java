package hyren.serv6.b.batchcollection.agentList;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.CodecUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.b.agent.bean.StoreConnectionBean;
import hyren.serv6.b.agent.tools.SendMsgUtil;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.fileutil.read.ReadLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Service
@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class AgentListService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getAgentInfoList() {
        return Dbo.queryResult("select ds.source_id, ds.datasource_name, " + " sum(case ai.agent_type when ? then 1 else 0 end) as dbflag, " + " sum(case ai.agent_type when ? then 1 else 0 end) as dfflag, " + " sum(case ai.agent_type when ? then 1 else 0 end) as nonstructflag," + " sum(case ai.agent_type when ? then 1 else 0 end) as halfstructflag," + " sum(case ai.agent_type when ? then 1 else 0 end) as ftpflag," + " sum(case ai.agent_type when ? then 1 else 0 end) as fileflag," + " sum(case ai.agent_type when ? then 1 else 0 end) as restflag" + " from " + DataSource.TableName + " ds " + " left join " + AgentInfo.TableName + " ai " + " on ds.source_id = ai.source_id" + " where ai.user_id = ?" + " group by ds.source_id order by datasource_name", AgentType.ShuJuKu.getCode(), AgentType.DBWenJian.getCode(), AgentType.WenJianXiTong.getCode(), AgentType.DuiXiang.getCode(), AgentType.FTP.getCode(), AgentType.WenBenLiu.getCode(), AgentType.XiaoXiLiu.getCode(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Param(name = "agentType", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getAgentInfo(long sourceId, String agentType) {
        Result result = Dbo.queryResult("select ai.*, ds.datasource_name FROM " + AgentInfo.TableName + " ai join " + DataSource.TableName + " ds on ai.source_id = ds.source_id WHERE ds.source_id = ? AND ai.agent_type = ? AND ai.user_id = ?", sourceId, agentType, UserUtil.getUserId());
        AgentType type = AgentType.ofEnumByCode(String.valueOf(agentType));
        for (int i = 0; i < result.getRowCount(); i++) {
            long agent_id = result.getLong(i, "agent_id");
            long num = 0;
            if (AgentType.ShuJuKu == type || AgentType.DBWenJian == type) {
                num = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where agent_id = ? and is_sendok = ?", agent_id, IsFlag.Shi.getCode()).orElse(0);
            } else if (AgentType.DuiXiang == type) {
                num = Dbo.queryNumber("select count(1) from " + ObjectCollect.TableName + " where agent_id = ? and is_sendok = ?", agent_id, IsFlag.Shi.getCode()).orElse(0);
            } else if (AgentType.FTP == type) {
                num = Dbo.queryNumber("select count(1) from " + FtpCollect.TableName + " where agent_id = ? and is_sendok = ?", agent_id, IsFlag.Shi.getCode()).orElse(0);
            } else if (AgentType.WenJianXiTong == type) {
                num = Dbo.queryNumber("select count(1) from " + FileCollectSet.TableName + " where agent_id = ? and is_sendok = ?", agent_id, IsFlag.Shi.getCode()).orElse(0);
            }
            result.setObject(i, "tasknum", num);
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Param(name = "agentId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getTaskInfo(long sourceId, long agentId) {
        Map<String, Object> agentInfo = Dbo.queryOneObject("SELECT ai.agent_type,ai.agent_id FROM " + DataSource.TableName + " ds JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id WHERE ds.source_id = ? AND ai.user_id = ? " + " AND ai.agent_id = ?", sourceId, UserUtil.getUserId(), agentId);
        if (agentInfo.isEmpty()) {
            throw new BusinessException("未找到Agent");
        }
        String sqlStr;
        AgentType agentType = AgentType.ofEnumByCode(String.valueOf(agentInfo.get("agent_type")));
        if (AgentType.ShuJuKu == agentType) {
            sqlStr = " SELECT ds.database_id ID,ds.task_name task_name,ds.agent_id AGENT_ID," + " gi.source_id source_id,ds.collect_type,gi.agent_type" + " FROM " + DatabaseSet.TableName + " ds " + " LEFT JOIN " + AgentInfo.TableName + " gi ON ds.Agent_id = gi.Agent_id " + " where ds.Agent_id = ? AND ds.is_sendok = ? ";
        } else if (AgentType.DBWenJian == agentType) {
            sqlStr = " SELECT ds.database_id ID,ds.task_name task_name,ds.agent_id AGENT_ID," + " gi.source_id source_id,gi.agent_type" + " FROM " + DatabaseSet.TableName + " ds " + " LEFT JOIN " + AgentInfo.TableName + " gi ON ds.Agent_id = gi.Agent_id " + " where ds.Agent_id = ?  AND ds.is_sendok = ?";
        } else if (AgentType.DuiXiang == agentType) {
            sqlStr = " SELECT fs.odc_id id,fs.obj_collect_name task_name,fs.AGENT_ID AGENT_ID,gi.source_id,gi.agent_type" + " FROM " + ObjectCollect.TableName + " fs " + " LEFT JOIN " + AgentInfo.TableName + " gi ON gi.Agent_id = fs.Agent_id " + " WHERE fs.Agent_id = ? AND fs.is_sendok = ?";
        } else if (AgentType.FTP == agentType) {
            sqlStr = " SELECT fs.ftp_id id,fs.ftp_name task_name,fs.AGENT_ID AGENT_ID,gi.source_id,gi.agent_type" + " FROM " + FtpCollect.TableName + " fs " + " LEFT JOIN " + AgentInfo.TableName + " gi ON gi.Agent_id = fs.Agent_id " + " WHERE fs.Agent_id = ? AND fs.is_sendok = ?";
        } else if (AgentType.WenJianXiTong == agentType) {
            sqlStr = " SELECT fs.fcs_id id,fs.fcs_name task_name,fs.AGENT_ID AGENT_ID,gi.source_id,gi.agent_type" + " FROM " + FileCollectSet.TableName + " fs " + " LEFT JOIN " + AgentInfo.TableName + " gi ON gi.Agent_id = fs.Agent_id " + " where fs.Agent_id = ? AND fs.is_sendok = ?";
        } else {
            throw new BusinessException("从数据库中取到的Agent类型不合法");
        }
        sqlStr += " ORDER BY task_name";
        return Dbo.queryResult(sqlStr, agentInfo.get("agent_id"), IsFlag.Shi.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "readNum", desc = "", range = "", nullable = true, valueIfNull = "100")
    @Return(desc = "", range = "")
    public String viewTaskLog(long agentId, int readNum) {
        if (readNum > 1000) {
            readNum = 1000;
        }
        return getTaskLog(agentId, readNum).get("log");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "readNum", desc = "", range = "", nullable = true, valueIfNull = "10000")
    public void downloadTaskLog(Long agentId, Integer readNum) {
        Map<String, String> taskLog = getTaskLog(agentId, readNum);
        byte[] bytes = taskLog.get("log").getBytes();
        File downloadFile = new File(taskLog.get("filePath"));
        responseFile(downloadFile.getName(), bytes);
    }

    private void responseFile(String fileName, byte[] bytes) {
        HttpServletResponse response = ContextDataHolder.getResponse();
        HttpServletRequest request = ContextDataHolder.getRequest();
        response.reset();
        try (OutputStream out = response.getOutputStream()) {
            if (request.getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                response.setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(), CodecUtil.GBK_STRING));
            } else {
                response.setHeader("content-disposition", "attachment;filename=" + URLEncoder.encode(fileName, CodecUtil.UTF8_STRING));
            }
            response.setContentType("APPLICATION/OCTET-STREAM");
            out.write(bytes);
            out.flush();
        } catch (IOException e) {
            throw new AppSystemException(e);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectSetId", desc = "", range = "")
    public void deleteHalfStructTask(long collectSetId) {
        long val = Dbo.queryNumber("select count(1) from " + DataSource.TableName + " ds " + " join " + AgentInfo.TableName + " ai on ai.source_id = ds.source_id " + " join " + ObjectCollect.TableName + " oc on ai.Agent_id = oc.Agent_id " + " where ai.user_id = ? and oc.odc_id = ?", UserUtil.getUserId(), collectSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 1) {
            throw new BusinessException("要删除的半结构化文件采集任务不存在");
        }
        DboExecute.deletesOrThrow("删除半结构化采集任务异常!", "delete from " + ObjectCollect.TableName + " where odc_id = ?", collectSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectSetId", desc = "", range = "")
    public void deleteFTPTask(long collectSetId) {
        long val = Dbo.queryNumber("select count(1) from " + DataSource.TableName + " ds " + " join " + AgentInfo.TableName + " ai on ai.source_id = ds.source_id " + " join " + FtpCollect.TableName + " fc on ai.Agent_id = fc.Agent_id " + " where ai.user_id = ? and fc.ftp_id = ?", UserUtil.getUserId(), collectSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 1) {
            throw new BusinessException("要删除的FTP采集任务不存在");
        }
        DboExecute.deletesOrThrow("删除FTP采集任务数据异常!", "delete from " + FtpCollect.TableName + " where ftp_id = ?", collectSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectSetId", desc = "", range = "")
    public void deleteDBTask(long collectSetId) {
        long val = Dbo.queryNumber("select count(1) from " + DataSource.TableName + " ds " + " join " + AgentInfo.TableName + " ai on ai.source_id = ds.source_id " + " join " + DatabaseSet.TableName + " dbs on ai.Agent_id = dbs.Agent_id " + " where ai.user_id = ? and dbs.database_id = ?", UserUtil.getUserId(), collectSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 1) {
            throw new BusinessException("要删除的数据库直连采集任务不存在");
        }
        DboExecute.deletesOrThrow("删除数据库直连采集任务异常!", "delete from " + DatabaseSet.TableName + " where database_id = ? ", collectSetId);
        List<Object> tableIds = Dbo.queryOneColumnList("select table_id from " + TableInfo.TableName + " where database_id = ?", collectSetId);
        if (!tableIds.isEmpty()) {
            for (Object tableId : tableIds) {
                deleteDirtyDataOfTb((long) tableId);
            }
        }
        Dbo.execute("delete from " + TableInfo.TableName + " where database_id = ? ", collectSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectSetId", desc = "", range = "")
    public void deleteDFTask(long collectSetId) {
        long val = Dbo.queryNumber("select count(1) from " + DataSource.TableName + " ds " + " join " + AgentInfo.TableName + " ai on ai.source_id = ds.source_id " + " join " + DatabaseSet.TableName + " dbs on ai.Agent_id = dbs.Agent_id " + " where ai.user_id = ? and dbs.database_id = ?", UserUtil.getUserId(), collectSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 1) {
            throw new BusinessException("要删除的数据文件采集任务不存在");
        }
        DboExecute.deletesOrThrow("删除数据文件采集任务异常!", "delete from " + DatabaseSet.TableName + " where database_id =?", collectSetId);
        List<Object> tableIds = Dbo.queryOneColumnList("select table_id from " + TableInfo.TableName + " where database_id = ?", collectSetId);
        if (!tableIds.isEmpty()) {
            for (Object tableId : tableIds) {
                deleteDirtyDataOfTb((long) tableId);
            }
        }
        Dbo.execute("delete from " + TableInfo.TableName + " where database_id = ? ", collectSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectSetId", desc = "", range = "")
    public void deleteNonStructTask(long collectSetId) {
        long val = Dbo.queryNumber("select count(1) from " + DataSource.TableName + " ds " + " join " + AgentInfo.TableName + " ai on ai.source_id = ds.source_id " + " join " + FileCollectSet.TableName + " fcs on ai.Agent_id = fcs.Agent_id " + " where ai.user_id = ? and fcs.fcs_id = ?", UserUtil.getUserId(), collectSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (val != 1) {
            throw new BusinessException("要删除的非结构化文件采集任务不存在");
        }
        DboExecute.deletesOrThrow("删除非结构化采集任务数据异常!", "delete  from " + FileCollectSet.TableName + " where fcs_id = ? ", collectSetId);
        int secNum = Dbo.execute("delete  from " + FileSource.TableName + " where fcs_id = ?", collectSetId);
        if (secNum == 0) {
            throw new BusinessException("删除非结构化采集任务异常!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result getProjectInfo() {
        return Dbo.queryResult("select etl_sys_id,etl_sys_cd,etl_sys_name from " + EtlSys.TableName + " where user_id = ?", UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getTaskInfoByTaskId(Long etl_sys_id) {
        return Dbo.queryResult(" select ess.sub_sys_id,ess.sub_sys_cd,ess.sub_sys_desc" + " from " + EtlSubSysList.TableName + " ess" + " join " + EtlSys.TableName + " es on es.etl_sys_id = ess.etl_sys_id" + " where es.user_id = ? and ess.etl_sys_id = ? ", UserUtil.getUserId(), etl_sys_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getDBAndDFTaskBySourceId(long sourceId) {
        return Dbo.queryResult("SELECT das.database_id " + "FROM " + DataSource.TableName + " ds " + "JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id " + "JOIN " + DatabaseSet.TableName + " das ON ai.agent_id = das.agent_id " + "WHERE ds.source_id = ? AND das.is_sendok = ? AND ai.user_id = ?", sourceId, IsFlag.Shi.getCode(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getNonStructTaskBySourceId(long sourceId) {
        return Dbo.queryResult("SELECT fcs.fcs_id " + "FROM " + DataSource.TableName + " ds " + "JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id " + "JOIN " + FileCollectSet.TableName + " fcs ON ai.agent_id = fcs.agent_id " + "WHERE ds.source_id = ? AND fcs.is_sendok = ? AND ai.user_id = ?", sourceId, IsFlag.Shi.getCode(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getHalfStructTaskBySourceId(long sourceId) {
        return Dbo.queryResult("SELECT fcs.odc_id " + "FROM " + DataSource.TableName + " ds " + "JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id " + "JOIN " + ObjectCollect.TableName + " fcs ON ai.agent_id = fcs.agent_id " + "WHERE ds.source_id = ? AND fcs.is_sendok = ? AND ai.user_id = ?", sourceId, IsFlag.Shi.getCode(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getFTPTaskBySourceId(long sourceId) {
        return Dbo.queryResult("SELECT fcs.ftp_id " + "FROM " + DataSource.TableName + " ds " + "JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id " + "JOIN " + FtpCollect.TableName + " fcs ON ai.agent_id = fcs.agent_id " + "WHERE ds.source_id = ? AND fcs.is_sendok = ? AND ai.user_id = ? ", sourceId, IsFlag.Shi.getCode(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "is_download", range = "", desc = "", nullable = true, valueIfNull = "false")
    @Param(name = "etl_date", range = "", desc = "", nullable = true, valueIfNull = "")
    @Param(name = "sqlParam", range = "", desc = "", nullable = true, valueIfNull = "")
    public void sendJDBCCollectTaskById(long colSetId, String is_download, String etl_date, String sqlParam) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到数据库采集任务");
        }
        Result sourceDBConfResult = Dbo.queryResult("SELECT dbs.agent_id, dbs.database_id, dbs.task_name," + " dbs.dsl_id, dbs.fetch_size," + " dbs.system_type, dbs.is_sendok, dbs.database_number, dbs.db_agent, dbs.plane_url," + " dbs.database_separatorr, dbs.row_separator, dbs.classify_id, ds.datasource_number," + " cjc.classify_num,dbs.collect_type FROM " + DataSource.TableName + " ds" + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + DatabaseSet.TableName + " dbs ON ai.agent_id = dbs.agent_id" + " left join " + CollectJobClassify.TableName + " cjc on dbs.classify_id = cjc.classify_id" + " where dbs.database_id = ?", colSetId);
        if (sourceDBConfResult.getRowCount() != 1) {
            throw new BusinessException("根据数据库采集任务ID查询到的任务配置信息不唯一");
        }
        List<Map<String, Object>> array = JsonUtil.toObject(sourceDBConfResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        Map<String, Object> sourceDBConfObj = array.get(0);
        StoreConnectionBean storeConnectionBean = SendMsgUtil.setStoreConnectionBean(Long.parseLong(sourceDBConfObj.get("dsl_id").toString()));
        sourceDBConfObj.put("jdbc_url", storeConnectionBean.getJdbc_url());
        sourceDBConfObj.put("database_drive", storeConnectionBean.getDatabase_driver());
        sourceDBConfObj.put("user_name", storeConnectionBean.getUser_name());
        sourceDBConfObj.put("database_pad", storeConnectionBean.getDatabase_pwd());
        sourceDBConfObj.put("database_type", storeConnectionBean.getDatabase_type());
        sourceDBConfObj.put("database_name", storeConnectionBean.getDatabase_name());
        sourceDBConfObj.put("signal_file_list", new ArrayList<>());
        Result collectTableResult = Dbo.queryResult("SELECT dbs.database_id, ti.table_id, ti.table_name, " + "ti.table_ch_name, ti.table_count, ti.source_tableid, ti.valid_s_date, ti.valid_e_date, ti.sql, " + "ti.remark, ti.is_user_defined, ti.is_md5,ti.is_register,ti.is_parallel,ti.page_sql,ti.rec_num_date," + "ti.unload_type,ti.is_customize_sql,ti.pageparallels, ti.dataincrement,tsi.storage_type, " + "tsi.storage_time, tsi.is_zipper, ds.datasource_number || '_' || cjc.classify_num || '_' || " + "ti.table_name as storage_table_name,ds.datasource_name,ai.agent_name,ai.agent_id,ai.user_id,ds.source_id" + " FROM " + DataSource.TableName + " ds " + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + DatabaseSet.TableName + " dbs ON ai.agent_id = dbs.agent_id" + " LEFT JOIN " + TableInfo.TableName + " ti on ti.database_id = dbs.database_id" + " LEFT JOIN " + CollectJobClassify.TableName + " cjc on dbs.classify_id = cjc.classify_id" + " LEFT JOIN " + TableStorageInfo.TableName + " tsi on tsi.table_id = ti.table_id" + " WHERE dbs.database_id = ?", colSetId);
        List<Map<String, Object>> collectTables = JsonUtil.toObject(collectTableResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        for (int i = 0; i < collectTables.size(); i++) {
            Map<String, Object> collectTable = collectTables.get(i);
            Long tableId = Long.parseLong(collectTable.get("table_id").toString());
            List<DataExtractionDef> data_extraction_defs = Dbo.queryList(DataExtractionDef.class, "select * from " + DataExtractionDef.TableName + " where table_id = ?", tableId);
            if (!data_extraction_defs.isEmpty()) {
                collectTable.put("data_extraction_def_list", data_extraction_defs);
            } else {
                collectTable.put("data_extraction_def_list", new ArrayList<>());
            }
            List<ColumnMerge> columnMerges = Dbo.queryList(ColumnMerge.class, "select * from " + ColumnMerge.TableName + " where table_id = ?", tableId);
            if (!columnMerges.isEmpty()) {
                collectTable.put("column_merge_list", columnMerges);
            } else {
                collectTable.put("column_merge_list", new ArrayList<>());
            }
            Result tableColResult = Dbo.queryResult("select tc.column_id, tc.is_primary_key, tc.column_name, tc.column_ch_name, " + " tc.valid_s_date, tc.valid_e_date, tc.is_get, tc.column_type, tc.tc_remark, tc.is_alive, " + " tc.is_new, tc.tc_or, tc.is_zipper_field from " + TableColumn.TableName + " tc where tc.table_id = ? and tc.is_get = ?", tableId, IsFlag.Shi.getCode());
            List<Map<String, Object>> tableColArray = JsonUtil.toObject(tableColResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
            });
            List<Map<String, Object>> tbcol_srctgt_maps = Dbo.queryList("select * from " + TbcolSrctgtMap.TableName + " where column_id in ( select column_id from " + TableColumn.TableName + " where table_id = ? and is_get = ?)", tableId, IsFlag.Shi.getCode());
            for (int j = 0; j < tableColArray.size(); j++) {
                Map<String, Object> tableColObj = tableColArray.get(j);
                Long columnId = Long.parseLong(tableColObj.get("column_id").toString());
                for (Map<String, Object> tbcol_srctgt_map : tbcol_srctgt_maps) {
                    if (tbcol_srctgt_map.get("column_id").equals(columnId)) {
                        tbcol_srctgt_map.put("column_name", tableColObj.get("column_name"));
                    }
                }
                Result columnCleanResult = Dbo.queryResult("select cc.col_clean_id, cc.clean_type ,cc.character_filling, cc.filling_length, " + " cc.field, cc.replace_feild, cc.filling_type, cc.convert_format, cc.old_format " + " from " + ColumnClean.TableName + " cc where cc.column_id = ?", columnId);
                List<Map<String, Object>> columnCleanArray = JsonUtil.toObject(columnCleanResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
                });
                long CVCount = Dbo.queryNumber("select count(1) from " + ColumnClean.TableName + " where clean_type = ? and column_id = ?", CleanType.MaZhiZhuanHuan.getCode(), columnId).orElseThrow(() -> new BusinessException("SQL查询错误"));
                if (CVCount > 0) {
                    for (int k = 0; k < columnCleanArray.size(); k++) {
                        Map columnCleanObj = columnCleanArray.get(k);
                        Long colCleanId = Long.parseLong(columnCleanObj.get("col_clean_id").toString());
                        Result CVCResult = Dbo.queryResult("select codename,codesys from " + ColumnClean.TableName + " where col_clean_id = ? and clean_type = ?", colCleanId, CleanType.MaZhiZhuanHuan.getCode());
                        if (CVCResult.getRowCount() == 1) {
                            Result CVResult = Dbo.queryResult("select code_value,orig_value from " + OrigCodeInfo.TableName + " where code_classify = ? and orig_sys_code = ?", CVCResult.getString(0, "codename"), CVCResult.getString(0, "codesys"));
                            columnCleanObj.put("codeTransform", JsonUtil.toJson(CVResult));
                        } else {
                            columnCleanObj.put("codeTransform", "");
                        }
                    }
                }
                long splitCount = Dbo.queryNumber("select count(1) from " + ColumnClean.TableName + " where clean_type = ? and column_id = ?", CleanType.ZiFuChaiFen.getCode(), columnId).orElseThrow(() -> new BusinessException("SQL查询错误"));
                if (splitCount > 0) {
                    for (int k = 0; k < columnCleanArray.size(); k++) {
                        Map columnCleanObj = columnCleanArray.get(k);
                        Long colCleanId = Long.parseLong(columnCleanObj.get("col_clean_id").toString());
                        List<ColumnSplit> columnSplits = Dbo.queryList(ColumnSplit.class, "select * from " + ColumnSplit.TableName + " where col_clean_id = ?", colCleanId);
                        if (!columnSplits.isEmpty()) {
                            columnCleanObj.put("column_split_list", columnSplits);
                        } else {
                            columnCleanObj.put("column_split_list", new ArrayList<>());
                        }
                    }
                }
                tableColObj.put("columnCleanBeanList", columnCleanArray);
            }
            collectTable.put("collectTableColumnBeanList", tableColArray);
            collectTable.put("tbColTarTypeMaps", tbcol_srctgt_maps);
        }
        sourceDBConfObj.put("collectTableBeanArray", collectTables);
        String methodName = AgentActionUtil.SENDJDBCCOLLECTTASKINFO;
        if (StringUtil.isNotBlank(etl_date)) {
            methodName = AgentActionUtil.JDBCCOLLECTEXECUTEIMMEDIATELY;
        }
        if ("true".equals(is_download)) {
            methodName = AgentActionUtil.GETDICTIONARYJSON;
        }
        String dataDic = (String) SendMsgUtil.sendDBCollectTaskInfo(Long.parseLong(sourceDBConfObj.get("database_id").toString()), Long.parseLong(sourceDBConfObj.get("agent_id").toString()), UserUtil.getUserId(), JsonUtil.toJson(sourceDBConfObj), methodName, etl_date, is_download, sqlParam);
        if ("true".equals(is_download)) {
            responseFile(sourceDBConfResult.getString(0, "task_name") + ".json", dataDic.getBytes());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "etl_date", desc = "", range = "", nullable = true, valueIfNull = "")
    public void sendDBCollectTaskById(long colSetId, String etl_date) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到数据库采集任务");
        }
        Map<String, Object> sourceDBConfObj = getJsonObject(colSetId);
        sourceDBConfObj.put("signal_file_list", new ArrayList<>());
        Result collectTableResult = Dbo.queryResult("SELECT dbs.dsl_id, dbs.database_id, ti.table_id, ti.table_name, " + "ti.table_ch_name, ti.table_count, ti.source_tableid, ti.valid_s_date, ti.valid_e_date, ti.sql, " + "ti.remark, ti.is_user_defined,case when tsi.is_zipper = ? then tsi.is_md5 else ti.is_md5 end,ti.is_register,ti.is_parallel,ti.page_sql,ti.rec_num_date," + "ti.unload_type,ti.is_customize_sql,ti.pageparallels, ti.dataincrement,tsi.storage_type, " + "tsi.storage_time, tsi.is_zipper, tsi.hyren_name as storage_table_name, ds.datasource_name, " + "ai.agent_name, ai.agent_id, ds.source_id, ai.user_id," + "dsr.storage_date FROM " + DataSource.TableName + " ds " + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + DatabaseSet.TableName + " dbs ON ai.agent_id = dbs.agent_id" + " LEFT JOIN " + TableInfo.TableName + " ti on ti.database_id = dbs.database_id" + " LEFT JOIN " + CollectJobClassify.TableName + " cjc on dbs.classify_id = cjc.classify_id" + " LEFT JOIN " + TableStorageInfo.TableName + " tsi on tsi.table_id = ti.table_id" + " LEFT JOIN " + DataStoreReg.TableName + " dsr on dsr.table_id = ti.table_id " + " WHERE dbs.database_id = ?", IsFlag.Fou.getCode(), colSetId);
        List<Map<String, Object>> collectTables = JsonUtil.toObject(collectTableResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        for (int i = 0; i < collectTables.size(); i++) {
            Map<String, Object> collectTable = collectTables.get(i);
            Long tableId = Long.parseLong(collectTable.get("table_id").toString());
            List<DataExtractionDef> data_extraction_defs = Dbo.queryList(DataExtractionDef.class, "select * from " + DataExtractionDef.TableName + " where table_id = ?", tableId);
            if (!data_extraction_defs.isEmpty()) {
                collectTable.put("data_extraction_def_list", data_extraction_defs);
            } else {
                collectTable.put("data_extraction_def_list", new ArrayList<>());
            }
            List<ColumnMerge> columnMerges = Dbo.queryList(ColumnMerge.class, "select * from " + ColumnMerge.TableName + " where table_id = ?", tableId);
            if (!columnMerges.isEmpty()) {
                collectTable.put("column_merge_list", columnMerges);
            } else {
                collectTable.put("column_merge_list", new ArrayList<>());
            }
            Result tableColResult = Dbo.queryResult("select tc.column_id, tc.is_primary_key, tc.column_name, tc.column_ch_name, " + " tc.valid_s_date, tc.valid_e_date, tc.is_get, tc.column_type, tc.tc_remark, tc.is_alive, " + " tc.is_new, tc.tc_or, tc.is_zipper_field from " + TableColumn.TableName + " tc where tc.table_id = ? and tc.is_get = ?", tableId, IsFlag.Shi.getCode());
            List<Map<String, Object>> tableColArray = JsonUtil.toObject(tableColResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
            });
            List<Map<String, Object>> tbcol_srctgt_maps = Dbo.queryList("select * from " + TbcolSrctgtMap.TableName + " where column_id in ( select column_id from " + TableColumn.TableName + " where table_id = ? and is_get = ?)", tableId, IsFlag.Shi.getCode());
            for (int j = 0; j < tableColArray.size(); j++) {
                Map<String, Object> tableColObj = tableColArray.get(j);
                Long columnId = Long.parseLong(tableColObj.get("column_id").toString());
                for (Map<String, Object> tbcol_srctgt_map : tbcol_srctgt_maps) {
                    if (tbcol_srctgt_map.get("column_id").equals(columnId)) {
                        tbcol_srctgt_map.put("column_name", tableColObj.get("column_name").toString());
                    }
                }
                Result columnCleanResult = Dbo.queryResult("select cc.col_clean_id, cc.clean_type ,cc.character_filling, cc.filling_length, " + " cc.field, cc.replace_feild, cc.filling_type, cc.convert_format, cc.old_format " + " from " + ColumnClean.TableName + " cc where cc.column_id = ?", columnId);
                List<Map<String, Object>> columnCleanArray = JsonUtil.toObject(columnCleanResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
                });
                long CVCount = Dbo.queryNumber("select count(1) from " + ColumnClean.TableName + " where clean_type = ? and column_id = ?", CleanType.MaZhiZhuanHuan.getCode(), columnId).orElseThrow(() -> new BusinessException("SQL查询错误"));
                if (CVCount > 0) {
                    for (int k = 0; k < columnCleanArray.size(); k++) {
                        Map<String, Object> columnCleanObj = columnCleanArray.get(k);
                        Long colCleanId = Long.parseLong(columnCleanObj.get("col_clean_id").toString());
                        Result CVCResult = Dbo.queryResult("select codename,codesys from " + ColumnClean.TableName + " where col_clean_id = ? and clean_type = ?", colCleanId, CleanType.MaZhiZhuanHuan.getCode());
                        if (CVCResult.getRowCount() == 1) {
                            Result CVResult = Dbo.queryResult("select code_value,orig_value from " + OrigCodeInfo.TableName + " where code_classify = ? and orig_sys_code = ?", CVCResult.getString(0, "codename"), CVCResult.getString(0, "codesys"));
                            columnCleanObj.put("codeTransform", JsonUtil.toJson(CVResult));
                        } else {
                            columnCleanObj.put("codeTransform", "");
                        }
                    }
                }
                long splitCount = Dbo.queryNumber("select count(1) from " + ColumnClean.TableName + " where clean_type = ? and column_id = ?", CleanType.ZiFuChaiFen.getCode(), columnId).orElseThrow(() -> new BusinessException("SQL查询错误"));
                if (splitCount > 0) {
                    for (int k = 0; k < columnCleanArray.size(); k++) {
                        Map<String, Object> columnCleanObj = columnCleanArray.get(k);
                        Long colCleanId = Long.parseLong(columnCleanObj.get("col_clean_id").toString());
                        List<ColumnSplit> columnSplits = Dbo.queryList(ColumnSplit.class, "select * from " + ColumnSplit.TableName + " where col_clean_id = ?", colCleanId);
                        if (!columnSplits.isEmpty()) {
                            columnCleanObj.put("column_split_list", columnSplits);
                        } else {
                            columnCleanObj.put("column_split_list", new ArrayList<>());
                        }
                    }
                }
                tableColObj.put("columnCleanBeanList", columnCleanArray);
            }
            collectTable.put("collectTableColumnBeanList", tableColArray);
            collectTable.put("tbColTarTypeMaps", tbcol_srctgt_maps);
            Result dataStoreResult = Dbo.queryResult("select dsl.dsl_id, dsl.dsl_name, dsl.store_type, dsl.is_hadoopclient " + " from " + DataStoreLayer.TableName + " dsl" + " where dsl.dsl_id in (select drt.dsl_id from " + DtabRelationStore.TableName + " drt where drt.tab_id = " + " (select storage_id from " + TableStorageInfo.TableName + " tsi where tsi.table_id = ?) AND drt.data_source = ?)", tableId, StoreLayerDataSource.DB.getCode());
            List<Map<String, Object>> dataStoreArray = JsonUtil.toObject(dataStoreResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
            });
            for (int m = 0; m < dataStoreArray.size(); m++) {
                Map<String, Object> dataStore = dataStoreArray.get(m);
                Long dslId = Long.parseLong(dataStore.get("dsl_id").toString());
                Result result = Dbo.queryResult("select storage_property_key, storage_property_val, is_file from " + DataStoreLayerAttr.TableName + " where dsl_id = ?", dslId);
                if (result.isEmpty()) {
                    throw new BusinessException("根据存储层配置ID" + dslId + "未获取到存储层配置属性信息");
                }
                Map<String, String> dataStoreConnectAttr = new HashMap<>();
                Map<String, String> dataStoreLayerFile = new HashMap<>();
                for (int n = 0; n < result.getRowCount(); n++) {
                    IsFlag fileFlag = IsFlag.ofEnumByCode(result.getString(n, "is_file"));
                    if (fileFlag == IsFlag.Shi) {
                        dataStoreLayerFile.put(result.getString(n, "storage_property_key"), result.getString(n, "storage_property_val"));
                    } else {
                        dataStoreConnectAttr.put(result.getString(n, "storage_property_key"), result.getString(n, "storage_property_val"));
                    }
                }
                dataStore.put("data_store_connect_attr", dataStoreConnectAttr);
                dataStore.put("data_store_layer_file", dataStoreLayerFile);
                List<Object> storeLayers = Dbo.queryOneColumnList("select dsla.dsla_storelayer from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla" + " on dsla.dslad_id = csi.dslad_id" + " where csi.col_id in" + " (select column_id from " + TableColumn.TableName + " where table_id = ?) " + " and dsla.dsl_id = ? AND csi.data_source = ?", tableId, dslId, StoreLayerDataSource.DB.getCode());
                Result columnResult = Dbo.queryResult("select dsla.dsla_storelayer, csi.csi_number, tc.column_name " + " from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla" + " on dsla.dslad_id = csi.dslad_id" + " join " + TableColumn.TableName + " tc " + " on csi.col_id = tc.column_id" + " where csi.col_id in (select column_id from " + TableColumn.TableName + " where table_id = ?) and dsla.dsl_id = ? AND csi.data_source = ?", tableId, dslId, StoreLayerDataSource.DB.getCode());
                Map<String, Map<String, Integer>> additInfoFieldMap = new HashMap<>();
                if (!columnResult.isEmpty() && !storeLayers.isEmpty()) {
                    for (Object obj : storeLayers) {
                        Map<String, Integer> fieldMap = new HashMap<>();
                        String storeLayer = (String) obj;
                        for (int h = 0; h < columnResult.getRowCount(); h++) {
                            String dslaStoreLayer = columnResult.getString(h, "dsla_storelayer");
                            if (storeLayer.equals(dslaStoreLayer)) {
                                fieldMap.put(columnResult.getString(h, "column_name"), columnResult.getInteger(h, "csi_number"));
                            }
                        }
                        additInfoFieldMap.put(storeLayer, fieldMap);
                    }
                } else {
                    defaultPrimaryKey(tableId, dslId, additInfoFieldMap);
                }
                dataStore.put("additInfoFieldMap", additInfoFieldMap);
            }
            collectTable.put("dataStoreConfBean", dataStoreArray);
        }
        StoreConnectionBean storeConnectionBean = SendMsgUtil.setStoreConnectionBean(Long.parseLong(sourceDBConfObj.get("dsl_id").toString()));
        sourceDBConfObj.put("jdbc_url", storeConnectionBean.getJdbc_url());
        sourceDBConfObj.put("database_drive", storeConnectionBean.getDatabase_driver());
        sourceDBConfObj.put("user_name", storeConnectionBean.getUser_name());
        sourceDBConfObj.put("database_pad", storeConnectionBean.getDatabase_pwd());
        sourceDBConfObj.put("database_type", storeConnectionBean.getDatabase_type());
        sourceDBConfObj.put("database_name", storeConnectionBean.getDatabase_name());
        sourceDBConfObj.put("collectTableBeanArray", collectTables);
        String methodName = AgentActionUtil.SENDDBCOLLECTTASKINFO;
        if (StringUtil.isNotBlank(etl_date)) {
            methodName = AgentActionUtil.DBCOLLECTEXECUTEIMMEDIATELY;
        }
        SendMsgUtil.sendDBCollectTaskInfo(Long.parseLong(sourceDBConfObj.get("database_id").toString()), Long.parseLong(sourceDBConfObj.get("agent_id").toString()), UserUtil.getUserId(), JsonUtil.toJson(sourceDBConfObj), methodName, etl_date, "false", "");
    }

    private Map<String, Object> getJsonObject(long colSetId) {
        Result sourceDBConfResult = Dbo.queryResult("SELECT dbs.dsl_id, dbs.agent_id, dbs.database_id, dbs.task_name, dbs.fetch_size," + " dbs.host_name, dbs.system_type, dbs.is_sendok, dbs.database_number, dbs.db_agent, dbs.plane_url," + " dbs.database_separatorr, dbs.row_separator, dbs.classify_id, ds.datasource_number," + " cjc.classify_num,dbs.collect_type FROM " + DataSource.TableName + " ds" + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + DatabaseSet.TableName + " dbs ON ai.agent_id = dbs.agent_id" + " left join " + CollectJobClassify.TableName + " cjc on dbs.classify_id = cjc.classify_id" + " where dbs.database_id = ?", colSetId);
        if (sourceDBConfResult.getRowCount() != 1) {
            throw new BusinessException("根据数据库采集任务ID查询到的任务配置信息不唯一");
        }
        List<Map<String, Object>> array = JsonUtil.toObject(sourceDBConfResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        Map<String, Object> sourceDBConfObj = array.get(0);
        StoreConnectionBean storeConnectionBean = SendMsgUtil.setStoreConnectionBean(Long.parseLong(sourceDBConfObj.get("dsl_id").toString()));
        sourceDBConfObj.put("jdbc_url", storeConnectionBean.getJdbc_url());
        sourceDBConfObj.put("database_drive", storeConnectionBean.getDatabase_driver());
        sourceDBConfObj.put("user_name", storeConnectionBean.getUser_name());
        sourceDBConfObj.put("database_pad", storeConnectionBean.getDatabase_pwd());
        sourceDBConfObj.put("database_type", storeConnectionBean.getDatabase_type());
        sourceDBConfObj.put("database_name", storeConnectionBean.getDatabase_name());
        return sourceDBConfObj;
    }

    private void defaultPrimaryKey(Long tableId, Long dslId, Map<String, Map<String, Integer>> additInfoFieldMap) {
        log.info("===============================" + "开始检查是否需要生成附加配置主键");
        List<String> storeLayers = Dbo.queryOneColumnList("SELECT  t2.dsla_storelayer  FROM " + DataStoreLayer.TableName + " t1" + " left join " + DataStoreLayerAdded.TableName + " t2 on t1.dsl_id = t2.dsl_id where  t1.dsl_id = ?  group by dsla_storelayer", dslId);
        StoreLayerAdded storeLayerAdded;
        for (String dsla_storelayer : storeLayers) {
            if (StringUtil.isNotBlank(dsla_storelayer)) {
                storeLayerAdded = StoreLayerAdded.ofEnumByCode(dsla_storelayer);
                if (storeLayerAdded == StoreLayerAdded.ZhuJian || storeLayerAdded == StoreLayerAdded.SuoYinLie) {
                    List<String> columnList = Dbo.queryOneColumnList("SELECT t1.column_name FROM " + TableColumn.TableName + " t1 join " + TableStorageInfo.TableName + " t2 ON t1.table_id = t2.table_id " + " WHERE t1.table_id = ? AND t1.is_primary_key = ? AND t2.is_zipper = ? ORDER BY column_id ", tableId, IsFlag.Shi.getCode(), IsFlag.Fou.getCode());
                    Map<String, Integer> fieldMap = new HashMap<>();
                    for (int j = 0; j < columnList.size(); j++) {
                        fieldMap.put(columnList.get(j), j);
                    }
                    additInfoFieldMap.put(dsla_storelayer, fieldMap);
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Param(name = "etl_date", desc = "", range = "", nullable = true, valueIfNull = "")
    public void sendObjectCollectTaskById(long odc_id, String etl_date) {
        long count = Dbo.queryNumber("select count(1) from " + ObjectCollect.TableName + " where odc_id = ?", odc_id).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到对象采集任务");
        }
        Result ObjectCollectResult = Dbo.queryResult("SELECT oc.*, ds.datasource_number FROM " + DataSource.TableName + " ds" + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + ObjectCollect.TableName + " oc ON ai.agent_id = oc.agent_id" + " where oc.odc_id = ?", odc_id);
        if (ObjectCollectResult.getRowCount() != 1) {
            throw new BusinessException("根据对象采集任务ID查询到的任务配置信息不唯一");
        }
        List<Map<String, Object>> array = JsonUtil.toObject(ObjectCollectResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        Map<String, Object> sourceDBConfObj = array.get(0);
        Result collectTableResult = Dbo.queryResult("SELECT oc.odc_id,oct.ocs_id,oct.en_name,oct.zh_name,oct.collect_data_type," + "oct.database_code,oct.updatetype, ds.datasource_name, " + "ai.agent_name, ai.agent_id, ds.source_id, ai.user_id," + "lower(ds.datasource_number || '_' || oc.obj_number || '_' ||  oct.en_name) " + "as hyren_name,dsr.storage_date FROM " + DataSource.TableName + " ds " + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + ObjectCollect.TableName + " oc ON ai.agent_id = oc.agent_id" + " LEFT JOIN " + ObjectCollectTask.TableName + " oct on oct.odc_id = oc.odc_id" + " LEFT JOIN " + DataStoreReg.TableName + " dsr on dsr.table_id = oct.ocs_id " + " WHERE oc.odc_id = ?", odc_id);
        List<Map<String, Object>> collectTables = JsonUtil.toObject(collectTableResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        for (int i = 0; i < collectTables.size(); i++) {
            Map<String, Object> collectTable = collectTables.get(i);
            Long tableId = Long.parseLong(collectTable.get("ocs_id").toString());
            List<ObjectHandleType> Object_handle_types = Dbo.queryList(ObjectHandleType.class, "select * from " + ObjectHandleType.TableName + " where ocs_id = ?", tableId);
            if (!Object_handle_types.isEmpty()) {
                collectTable.put("object_handle_typeList", Object_handle_types);
            } else {
                collectTable.put("object_handle_typeList", new ArrayList<>());
            }
            List<ObjectCollectStruct> Object_collect_structs = Dbo.queryList(ObjectCollectStruct.class, "select * from " + ObjectCollectStruct.TableName + " where ocs_id = ?", tableId);
            if (!Object_collect_structs.isEmpty()) {
                collectTable.put("object_collect_structList", Object_collect_structs);
            } else {
                collectTable.put("object_collect_structList", new ArrayList<>());
            }
            Result dataStoreResult = Dbo.queryResult("select dsl.dsl_id, dsl.dsl_name, dsl.store_type, dsl.is_hadoopclient" + " from " + DataStoreLayer.TableName + " dsl" + " where dsl.dsl_id in (select drt.dsl_id from " + DtabRelationStore.TableName + " drt where drt.tab_id = ?" + " AND drt.data_source = ?)", tableId, StoreLayerDataSource.OBJ.getCode());
            List<Map<String, Object>> dataStoreArray = JsonUtil.toObject(dataStoreResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
            });
            for (int m = 0; m < dataStoreArray.size(); m++) {
                Map<String, Object> dataStore = dataStoreArray.get(m);
                Long dslId = Long.parseLong(dataStore.get("dsl_id").toString());
                Result result = Dbo.queryResult("select storage_property_key, storage_property_val, is_file from " + DataStoreLayerAttr.TableName + " where dsl_id = ?", dslId);
                if (result.isEmpty()) {
                    throw new BusinessException("根据存储层配置ID" + dslId + "未获取到存储层配置属性信息");
                }
                Map<String, String> dataStoreConnectAttr = new HashMap<>();
                Map<String, String> dataStoreLayerFile = new HashMap<>();
                for (int n = 0; n < result.getRowCount(); n++) {
                    IsFlag fileFlag = IsFlag.ofEnumByCode(result.getString(n, "is_file"));
                    if (fileFlag == IsFlag.Shi) {
                        dataStoreLayerFile.put(result.getString(n, "storage_property_key"), result.getString(n, "storage_property_val"));
                    } else {
                        dataStoreConnectAttr.put(result.getString(n, "storage_property_key"), result.getString(n, "storage_property_val"));
                    }
                }
                dataStore.put("data_store_connect_attr", dataStoreConnectAttr);
                dataStore.put("data_store_layer_file", dataStoreLayerFile);
                List<Object> storeLayers = Dbo.queryOneColumnList("select dsla.dsla_storelayer from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla" + " on dsla.dslad_id = csi.dslad_id" + " where csi.col_id in" + " (select struct_id from " + ObjectCollectStruct.TableName + " where ocs_id = ?) " + " and dsla.dsl_id = ? AND csi.data_source = ?", tableId, dslId, StoreLayerDataSource.OBJ.getCode());
                Result columnResult = Dbo.queryResult("select dsla.dsla_storelayer, csi.csi_number, tc.column_name " + " from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla" + " on dsla.dslad_id = csi.dslad_id" + " join " + ObjectCollectStruct.TableName + " tc " + " on csi.col_id = tc.struct_id" + " where csi.col_id in (select struct_id from " + ObjectCollectStruct.TableName + " where ocs_id = ?) and dsla.dsl_id = ? AND csi.data_source = ?", tableId, dslId, StoreLayerDataSource.OBJ.getCode());
                Map<String, Map<String, Integer>> additInfoFieldMap = new HashMap<>();
                if (!columnResult.isEmpty() && !storeLayers.isEmpty()) {
                    for (Object obj : storeLayers) {
                        Map<String, Integer> fieldMap = new HashMap<>();
                        String storeLayer = (String) obj;
                        for (int h = 0; h < columnResult.getRowCount(); h++) {
                            String dslaStoreLayer = columnResult.getString(h, "dsla_storelayer");
                            if (storeLayer.equals(dslaStoreLayer)) {
                                fieldMap.put(columnResult.getString(h, "column_name"), columnResult.getInteger(h, "csi_number"));
                            }
                        }
                        additInfoFieldMap.put(storeLayer, fieldMap);
                    }
                }
                dataStore.put("additInfoFieldMap", additInfoFieldMap);
            }
            collectTable.put("dataStoreConfBean", dataStoreArray);
        }
        sourceDBConfObj.put("objectTableBeanList", collectTables);
        String methodName = AgentActionUtil.OBJECTCOLLECTEXECUTE;
        if (StringUtil.isNotBlank(etl_date)) {
            methodName = AgentActionUtil.OBJECTCOLLECTEXECUTEIMMEDIATELY;
        }
        SendMsgUtil.sendObjectCollectTaskInfo(Long.parseLong(sourceDBConfObj.get("odc_id").toString()), Long.parseLong(sourceDBConfObj.get("agent_id").toString()), UserUtil.getUserId(), JsonUtil.toJson(sourceDBConfObj), methodName, etl_date);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ftp_id", desc = "", range = "")
    public void sendFtpCollect(long ftp_id) {
        FtpCollect ftpCollect = Dbo.queryOneObject(FtpCollect.class, "SELECT * FROM " + FtpCollect.TableName + " WHERE ftp_id = ?", ftp_id).orElseThrow(() -> new BusinessException(String.format("根据任务id:%s查询ftp任务失败", ftp_id)));
        String url = AgentActionUtil.getUrl(ftpCollect.getAgent_id(), getUserId(), AgentActionUtil.SENDFTPCOLLECTTASKINFO);
        String ftp_collect_info = JsonUtil.toJson(ftpCollect);
        log.info("配置的ftp采集信息" + ftp_collect_info);
        try {
            HttpClient.ResponseValue resVal = new HttpClient().addData("taskInfo", ftp_collect_info).post(url);
            ActionResult ar = ActionResult.toActionResult(resVal.getBodyString());
            if (!ar.isSuccess()) {
                throw new BusinessException("连接" + url + "失败");
            }
        } catch (Exception e) {
            throw new BusinessException("与Agent端交互异常!!!" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "agentId", desc = "", range = "")
    @Param(name = "readNum", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, String> getTaskLog(long agentId, int readNum) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + AgentInfo.TableName + " WHERE agent_id = ?", agentId).orElseThrow(() -> new BusinessException("查询Agent信息的SQL错误"));
        if (countNum == 0) {
            throw new BusinessException("为获取到Agent(" + agentId + ")的信息");
        }
        AgentInfo agent_info = Dbo.queryOneObject(AgentInfo.class, "SELECT * FROM " + AgentInfo.TableName + " WHERE agent_id = ?", agentId).orElseThrow(() -> new BusinessException("根据AgentId(" + agentId + ")获取的信息出现了多条"));
        List<AgentDownInfo> list = Dbo.queryList(AgentDownInfo.class, "SELECT * FROM " + AgentDownInfo.TableName + " WHERE agent_ip = ? AND agent_port = ?", agent_info.getAgent_ip(), agent_info.getAgent_port());
        if (list.isEmpty()) {
            throw new BusinessException("根据Agent IP(" + agent_info.getAgent_ip() + "),端口(" + agent_info.getAgent_port() + "),为获取到对应的部署信息");
        }
        AgentDownInfo agent_down_info = list.get(0);
        if (StringUtil.isEmpty(agent_down_info.getLog_dir())) {
            throw new BusinessException("Agent部署的日志路径是空的");
        }
        if (StringUtil.isEmpty(agent_down_info.getAgent_ip())) {
            throw new BusinessException("Agent部署的IP是空的");
        }
        if (StringUtil.isEmpty(agent_down_info.getAgent_port())) {
            throw new BusinessException("Agent部署的端口是空的");
        }
        if (StringUtil.isEmpty(agent_down_info.getUser_name())) {
            throw new BusinessException("Agent部署用户名是空的");
        }
        if (StringUtil.isEmpty(agent_down_info.getPasswd())) {
            throw new BusinessException("Agent部署的用户密码是空的");
        }
        SSHDetails sshDetails = new SSHDetails();
        sshDetails.setHost(agent_down_info.getAgent_ip());
        sshDetails.setPort(CommonVariables.SFTP_PORT);
        sshDetails.setUser_name(agent_down_info.getUser_name());
        sshDetails.setPwd(agent_down_info.getPasswd());
        String taskLog = ReadLog.readAgentLog(agent_down_info.getLog_dir(), sshDetails, readNum);
        if (StringUtil.isBlank(taskLog)) {
            taskLog = "未获取到日志";
        }
        Map<String, String> map = new HashMap<>();
        map.put("log", taskLog);
        map.put("filePath", agent_down_info.getLog_dir());
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    private void deleteDirtyDataOfTb(long tableId) {
        List<Object> columnIds = Dbo.queryOneColumnList("select column_id from " + TableColumn.TableName + " WHERE table_id = ?", tableId);
        if (!columnIds.isEmpty()) {
            for (Object columnId : columnIds) {
                deleteDirtyDataOfCol((long) columnId);
            }
        }
        Dbo.execute(" DELETE FROM " + TableColumn.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + DataExtractionDef.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + DtabRelationStore.TableName + " WHERE tab_id = " + "(SELECT storage_id FROM " + TableStorageInfo.TableName + " WHERE table_id = ?) AND data_source = ? ", tableId, StoreLayerDataSource.DB.getCode());
        Dbo.execute(" DELETE FROM " + TableStorageInfo.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + ColumnMerge.TableName + " WHERE table_id = ? ", tableId);
        Dbo.execute(" DELETE FROM " + TableClean.TableName + " WHERE table_id = ? ", tableId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    private void deleteDirtyDataOfCol(long columnId) {
        Dbo.execute("delete from " + DcolRelationStore.TableName + " where col_id = ? AND data_source = ?", columnId, StoreLayerDataSource.DB.getCode());
        Dbo.execute("delete from " + ColumnClean.TableName + " where column_id = ?", columnId);
        Dbo.execute("delete from " + ColumnSplit.TableName + " where column_id = ?", columnId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<AgentDownInfo> agentDeployData(long agent_id) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + AgentInfo.TableName + " WHERE agent_id = ?", agent_id).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 0) {
            throw new BusinessException("未找到AgentId(" + agent_id + ")的信息");
        }
        Map<String, Object> agentMap = Dbo.queryOneObject("SELECT agent_ip,agent_port FROM " + AgentInfo.TableName + " WHERE agent_id = ?", agent_id);
        countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + AgentDownInfo.TableName + " WHERE agent_ip = ? AND agent_port = ?", agentMap.get("agent_ip"), agentMap.get("agent_port")).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 0) {
            throw new BusinessException(String.format("未找到Agent的部署信息,IP: %s,端口: %s", agentMap.get("agnet_ip"), agentMap.get("agent_port")));
        }
        return Dbo.queryList(AgentDownInfo.class, "SELECT agent_ip,log_dir FROM " + AgentDownInfo.TableName + " WHERE agent_ip = ? AND agent_port = ?", agentMap.get("agent_ip"), agentMap.get("agent_port"));
    }

    public String getSqlParamPlaceholder() {
        return Constant.SQLDELIMITER;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Param(name = "targetPath", desc = "", range = "")
    @Param(name = "agent_id", desc = "", range = "")
    public String uploadDataDictionary(MultipartFile file, String targetPath, long agent_id) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + AgentInfo.TableName + " WHERE agent_id = ?", agent_id).orElseThrow(() -> new BusinessException("SQL异常"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("此Agent ID(%s)不存在", agent_id);
        }
        Map<String, Object> agentMap = Dbo.queryOneObject("SELECT t1.agent_ip,t1.agent_port,t1.user_name,t1.passwd FROM " + AgentDownInfo.TableName + " t1 JOIN " + AgentInfo.TableName + " t2 ON t1.agent_ip = t2.agent_ip AND t1.agent_port = t2.agent_port where t2.agent_id = ?", agent_id);
        String originalFilename = file.getOriginalFilename();
        SSHDetails sshDetails = new SSHDetails();
        sshDetails.setHost(agentMap.get("agent_ip").toString());
        sshDetails.setPort(CommonVariables.SFTP_PORT);
        sshDetails.setUser_name(agentMap.get("user_name").toString());
        sshDetails.setPwd(agentMap.get("passwd").toString());
        File uploadDir = null;
        File uploadedFile = null;
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            uploadDir = new File(WebinfoProperties.FileUpload_SavedDirName + targetPath);
            log.info("=======uploadDir：{}=========", uploadDir);
            if (!uploadDir.exists()) {
                if (!uploadDir.mkdirs()) {
                    throw new BusinessException(String.format("创建目录%s失败,检查当前用户是否有创建目录权限：", uploadDir.getAbsolutePath()));
                }
            }
            uploadedFile = new File(WebinfoProperties.FileUpload_SavedDirName + targetPath + File.separator + originalFilename);
            file.transferTo(uploadedFile);
            String upFilePath;
            if (!uploadedFile.exists()) {
                CheckParam.throwErrorMsg("上传的数据字典不存在:" + uploadedFile.getAbsolutePath());
            } else {
                upFilePath = uploadedFile.getAbsolutePath();
                if (upFilePath.lastIndexOf(uploadedFile.getName()) >= 0) {
                    upFilePath = upFilePath.substring(0, upFilePath.lastIndexOf(uploadedFile.getName())) + originalFilename;
                    if (!uploadedFile.renameTo(new File(upFilePath))) {
                        throw new BusinessException("文件: '" + uploadedFile.getName() + "' 重命名为 '" + upFilePath + "' 失败!");
                    }
                }
                sshOperate.execCommandBySSHNoRs("mkdir -p " + targetPath);
                log.info("===upFilePath:{}====", upFilePath);
                log.info("===targetPath:{}====", targetPath);
                sshOperate.channelSftp.put(upFilePath, targetPath);
            }
        } catch (Exception e) {
            throw new BusinessException(String.format("上传的数据字典文件失败:%s", e));
        } finally {
            if (uploadDir != null) {
                uploadDir.deleteOnExit();
            }
            if (uploadedFile != null) {
                uploadedFile.deleteOnExit();
            }
        }
        return targetPath + File.separator + originalFilename;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "etl_date", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "sqlParam", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    public void sendCollectDatabase(long colSetId, String etl_date, String sqlParam, Long user_id) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到数据库采集任务");
        }
        Result sourceDBConfResult = Dbo.queryResult("SELECT dbs.dsl_id, dbs.agent_id, dbs.database_id, dbs.task_name, dbs.fetch_size," + " dbs.host_name, dbs.system_type, dbs.is_sendok, dbs.database_number, dbs.db_agent, dbs.plane_url," + " dbs.database_separatorr, dbs.row_separator, dbs.classify_id, ds.datasource_number," + " cjc.classify_num,dbs.collect_type FROM " + DataSource.TableName + " ds" + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + DatabaseSet.TableName + " dbs ON ai.agent_id = dbs.agent_id" + " left join " + CollectJobClassify.TableName + " cjc on dbs.classify_id = cjc.classify_id" + " where dbs.database_id = ?", colSetId);
        if (sourceDBConfResult.getRowCount() != 1) {
            throw new BusinessException("根据数据库采集任务ID查询到的任务配置信息不唯一");
        }
        List<Map<String, Object>> array = JsonUtil.toObject(sourceDBConfResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        Map<String, Object> sourceDBConfObj = array.get(0);
        StoreConnectionBean storeConnectionBean = SendMsgUtil.setStoreConnectionBean(Long.parseLong(sourceDBConfObj.get("dsl_id").toString()));
        sourceDBConfObj.put("jdbc_url", storeConnectionBean.getJdbc_url());
        sourceDBConfObj.put("database_drive", storeConnectionBean.getDatabase_driver());
        sourceDBConfObj.put("user_name", storeConnectionBean.getUser_name());
        sourceDBConfObj.put("database_pad", storeConnectionBean.getDatabase_pwd());
        sourceDBConfObj.put("database_type", storeConnectionBean.getDatabase_type());
        sourceDBConfObj.put("database_name", storeConnectionBean.getDatabase_name());
        sourceDBConfObj.put("signal_file_list", new ArrayList<>());
        Result collectTableResult = Dbo.queryResult("SELECT dbs.database_id, ti.table_id, ti.table_name, " + "ti.table_ch_name, ti.table_count, ti.source_tableid, ti.valid_s_date, ti.valid_e_date, ti.sql, " + "ti.remark, ti.is_user_defined,  case when tsi.is_zipper = ? then tsi.is_md5 else ti.is_md5 end," + "ti.is_register,ti.is_parallel,ti.page_sql,ti.rec_num_date," + "ti.unload_type,ti.is_customize_sql,ti.pageparallels, ti.dataincrement,tsi.storage_type, " + "tsi.storage_time, tsi.is_zipper, tsi.hyren_name as storage_table_name, ds.datasource_name, " + "ai.agent_name, ai.agent_id, ds.source_id, ai.user_id,tc.interval_time,tc.over_date," + "dsr.storage_date FROM " + DataSource.TableName + " ds " + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + DatabaseSet.TableName + " dbs ON ai.agent_id = dbs.agent_id" + " LEFT JOIN " + TableInfo.TableName + " ti on ti.database_id = dbs.database_id" + " LEFT JOIN " + TableCycle.TableName + " tc on ti.table_id = tc.table_id" + " LEFT JOIN " + CollectJobClassify.TableName + " cjc on dbs.classify_id = cjc.classify_id" + " LEFT JOIN " + TableStorageInfo.TableName + " tsi on tsi.table_id = ti.table_id" + " LEFT JOIN " + DataStoreReg.TableName + " dsr on dsr.table_id = ti.table_id " + " WHERE dbs.database_id = ?", IsFlag.Fou.getCode(), colSetId);
        List<Map<String, Object>> collectTables = JsonUtil.toObject(collectTableResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        for (int i = 0; i < collectTables.size(); i++) {
            Map<String, Object> collectTable = collectTables.get(i);
            Long tableId = Long.parseLong(collectTable.get("table_id").toString());
            List<ColumnMerge> columnMerges = Dbo.queryList(ColumnMerge.class, "select * from " + ColumnMerge.TableName + " where table_id = ?", tableId);
            if (!columnMerges.isEmpty()) {
                collectTable.put("column_merge_list", columnMerges);
            } else {
                collectTable.put("column_merge_list", new ArrayList<>());
            }
            Result tableColResult = Dbo.queryResult("select tc.column_id, tc.is_primary_key, tc.column_name, tc.column_ch_name, " + " tc.valid_s_date, tc.valid_e_date, tc.is_get, tc.column_type, tc.tc_remark, tc.is_alive, " + " tc.is_new, tc.tc_or from " + TableColumn.TableName + " tc where tc.table_id = ? and tc.is_get = ?", tableId, IsFlag.Shi.getCode());
            List<Map<String, Object>> tableColArray = JsonUtil.toObject(tableColResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
            });
            collectTable.put("collectTableColumnBeanList", tableColArray);
            List<Map<String, Object>> tbcol_srctgt_maps = Dbo.queryList("select * from " + TbcolSrctgtMap.TableName + " where column_id in ( select column_id from " + TableColumn.TableName + " where table_id = ? and is_get = ?)", tableId, IsFlag.Shi.getCode());
            for (int j = 0; j < tableColArray.size(); j++) {
                Map<String, Object> tableColObj = tableColArray.get(j);
                Long columnId = Long.parseLong(tableColObj.get("column_id").toString());
                for (Map<String, Object> tbcol_srctgt_map : tbcol_srctgt_maps) {
                    if (tbcol_srctgt_map.get("column_id").equals(columnId)) {
                        tbcol_srctgt_map.put("column_name", tableColObj.get("column_name"));
                    }
                }
            }
            collectTable.put("tbColTarTypeMaps", tbcol_srctgt_maps);
            Result dataStoreResult = Dbo.queryResult("select dsl.dsl_id, dsl.dsl_name, dsl.store_type, dsl.is_hadoopclient " + " from " + DataStoreLayer.TableName + " dsl" + " where dsl.dsl_id in (select drt.dsl_id from " + DtabRelationStore.TableName + " drt where drt.tab_id = " + " (select storage_id from " + TableStorageInfo.TableName + " tsi where tsi.table_id = ?) AND drt.data_source = ?)", tableId, StoreLayerDataSource.DB.getCode());
            List<Map<String, Object>> dataStoreArray = JsonUtil.toObject(dataStoreResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
            });
            for (int m = 0; m < dataStoreArray.size(); m++) {
                Map<String, Object> dataStore = dataStoreArray.get(m);
                Long dslId = Long.parseLong(dataStore.get("dsl_id").toString());
                Result result = Dbo.queryResult("select storage_property_key, storage_property_val, is_file from " + DataStoreLayerAttr.TableName + " where dsl_id = ?", dslId);
                if (result.isEmpty()) {
                    throw new BusinessException("根据存储层配置ID" + dslId + "未获取到存储层配置属性信息");
                }
                Map<String, String> dataStoreConnectAttr = new HashMap<>();
                Map<String, String> dataStoreLayerFile = new HashMap<>();
                for (int n = 0; n < result.getRowCount(); n++) {
                    IsFlag fileFlag = IsFlag.ofEnumByCode(result.getString(n, "is_file"));
                    if (fileFlag == IsFlag.Shi) {
                        dataStoreLayerFile.put(result.getString(n, "storage_property_key"), result.getString(n, "storage_property_val"));
                    } else {
                        dataStoreConnectAttr.put(result.getString(n, "storage_property_key"), result.getString(n, "storage_property_val"));
                    }
                }
                dataStore.put("data_store_connect_attr", dataStoreConnectAttr);
                dataStore.put("data_store_layer_file", dataStoreLayerFile);
                List<Object> storeLayers = Dbo.queryOneColumnList("select dsla.dsla_storelayer from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla" + " on dsla.dslad_id = csi.dslad_id" + " where csi.col_id in" + " (select column_id from " + TableColumn.TableName + " where table_id = ?) " + " and dsla.dsl_id = ? AND csi.data_source = ?", tableId, dslId, StoreLayerDataSource.DB.getCode());
                Result columnResult = Dbo.queryResult("select dsla.dsla_storelayer, csi.csi_number, tc.column_name " + " from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla" + " on dsla.dslad_id = csi.dslad_id" + " join " + TableColumn.TableName + " tc " + " on csi.col_id = tc.column_id" + " where csi.col_id in (select column_id from " + TableColumn.TableName + " where table_id = ?) and dsla.dsl_id = ? AND csi.data_source = ?", tableId, dslId, StoreLayerDataSource.DB.getCode());
                Map<String, Map<String, Integer>> additInfoFieldMap = new HashMap<>();
                if (!columnResult.isEmpty() && !storeLayers.isEmpty()) {
                    for (Object obj : storeLayers) {
                        Map<String, Integer> fieldMap = new HashMap<>();
                        String storeLayer = (String) obj;
                        for (int h = 0; h < columnResult.getRowCount(); h++) {
                            String dslaStoreLayer = columnResult.getString(h, "dsla_storelayer");
                            if (storeLayer.equals(dslaStoreLayer)) {
                                fieldMap.put(columnResult.getString(h, "column_name"), columnResult.getInteger(h, "csi_number"));
                            }
                        }
                        additInfoFieldMap.put(storeLayer, fieldMap);
                    }
                } else {
                    defaultPrimaryKey(tableId, dslId, additInfoFieldMap);
                }
                dataStore.put("additInfoFieldMap", additInfoFieldMap);
            }
            collectTable.put("dataStoreConfBean", dataStoreArray);
        }
        sourceDBConfObj.put("collectTableBeanArray", collectTables);
        String methodName = AgentActionUtil.SENDDBCOLLECTTASKINFO;
        if (StringUtil.isNotBlank(etl_date)) {
            methodName = AgentActionUtil.JDBCDIRECTEXECUTEIMMEDIATELY;
        }
        SendMsgUtil.sendDBCollectTaskInfo(Long.parseLong(sourceDBConfObj.get("database_id").toString()), Long.parseLong(sourceDBConfObj.get("agent_id").toString()), UserUtil.getUserId() == null ? user_id : UserUtil.getUserId(), JsonUtil.toJson(sourceDBConfObj), methodName, etl_date, "false", sqlParam);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean startJobType(long colSetId) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("当前任务ID(%s)不存在", colSetId);
        }
        countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + TakeRelationEtl.TableName + " tre" + " JOIN " + DataExtractionDef.TableName + " ded ON tre.take_id = ded.ded_id " + " JOIN " + TableInfo.TableName + " ti ON ded.table_id = ti.table_id " + " WHERE ti.database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        return countNum == 0;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "odc_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean startObjJobType(long odc_id) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + ObjectCollect.TableName + " WHERE odc_id = ?", odc_id).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("当前任务ID(%s)不存在", odc_id);
        }
        countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + TakeRelationEtl.TableName + " tre JOIN " + ObjectCollectTask.TableName + " oct ON tre.take_id = oct.odc_id " + " WHERE oct.odc_id = ?", odc_id).orElseThrow(() -> new BusinessException("SQL查询错误"));
        return countNum == 0;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public String getDatabaseData() {
        List<Map<String, Object>> databaseList = Dbo.queryList("SELECT DISTINCT t1.dsl_id FROM " + DatabaseSet.TableName + " t1 JOIN " + AgentInfo.TableName + " t2 ON t1.agent_id = t2.agent_id WHERE t2.agent_type = ? AND t2.user_id = ? AND collect_type in (?,?)" + " GROUP BY " + "t1.dsl_id", AgentType.ShuJuKu.getCode(), UserUtil.getUserId(), CollectType.ShuJuKuCaiJi.getCode(), CollectType.ShuJuKuChouShu.getCode());
        return AesUtil.encrypt(JsonUtil.toJson(databaseList));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, List<Object>> getStoreDataBase(long colSetId) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("任务ID( " + colSetId + " )不存在)"));
        if (countNum == 0) {
            throw new BusinessException("任务ID( " + colSetId + " )不存在)");
        }
        List<Map<String, Object>> queryList = Dbo.queryList("SELECT t1.table_id,t3.dsl_name FROM " + TableStorageInfo.TableName + " t1 JOIN " + DtabRelationStore.TableName + " t2 ON " + " t1.storage_id = t2.tab_id JOIN " + DataStoreLayer.TableName + " t3 ON t2.dsl_id = t3.dsl_id " + "WHERE t1.table_id IN (SELECT ti.table_id FROM " + TableInfo.TableName + " ti JOIN " + DataExtractionDef.TableName + " ded ON ti.table_id = ded.table_id WHERE ti.database_id = ? and ded.data_extract_type = ? " + "ORDER BY ti.table_name)", colSetId, DataExtractType.YuanShuJuGeShi.getCode());
        Map<String, List<Object>> map = new HashMap<>();
        queryList.forEach(itemMap -> {
            String table_id = itemMap.get("table_id").toString();
            if (map.containsKey(table_id)) {
                map.get(table_id).add(itemMap.get("dsl_name"));
            } else {
                List<Object> list = new ArrayList<>();
                list.add(itemMap.get("dsl_name"));
                map.put(table_id, list);
            }
        });
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "database_id", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "collect_type", desc = "", range = "")
    @Param(name = "etl_date", desc = "", range = "")
    @Param(name = "file_type", desc = "", range = "")
    @Param(name = "sql_para", desc = "", range = "", nullable = true, valueIfNull = "")
    public void startTableCollect(long database_id, String table_name, String collect_type, String etl_date, String file_type, String sql_para) {
        Validator.notNull(database_id, "任务ID不能为空");
        Validator.notBlank(table_name, "采集表名称不能为空");
        Validator.notBlank(collect_type, "采集类型不能为空");
        Validator.notBlank(etl_date, "跑批日期不能为空");
        Validator.notBlank(file_type, "文件格式类型不能为空");
        sendDBCollectTaskById(database_id, "");
        Map<String, Object> sourceDBConfObj = getJsonObject(database_id);
        SendMsgUtil.startSingleJob(database_id, table_name, collect_type, etl_date, file_type, sql_para, Long.parseLong(sourceDBConfObj.get("agent_id").toString()), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "etl_date", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "sqlParam", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    public void sendCollectKafKaDatabase(Long colSetId, String etl_date, String sqlParam, Long user_id) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到数据库采集任务");
        }
        Result sourceDBConfResult = Dbo.queryResult("SELECT dbs_dsl_id, dbs.agent_id, dbs.database_id, dbs.task_name, dbs.fetch_size," + "  dbs.system_type, dbs.is_sendok, dbs.database_number, dbs.db_agent, dbs.plane_url," + " dbs.database_separatorr, dbs.row_separator, dbs.classify_id, ds.datasource_number," + " cjc.classify_num,dbs.collect_type FROM " + DataSource.TableName + " ds" + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + DatabaseSet.TableName + " dbs ON ai.agent_id = dbs.agent_id" + " left join " + CollectJobClassify.TableName + " cjc on dbs.classify_id = cjc.classify_id" + " where dbs.database_id = ?", colSetId);
        if (sourceDBConfResult.getRowCount() != 1) {
            throw new BusinessException("根据数据库采集任务ID查询到的任务配置信息不唯一");
        }
        List<Map<String, Object>> array = JsonUtil.toObject(sourceDBConfResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        Map<String, Object> sourceDBConfObj = array.get(0);
        StoreConnectionBean storeConnectionBean = SendMsgUtil.setStoreConnectionBean(Long.parseLong(sourceDBConfObj.get("dsl_id").toString()));
        sourceDBConfObj.put("jdbc_url", storeConnectionBean.getJdbc_url());
        sourceDBConfObj.put("database_drive", storeConnectionBean.getDatabase_driver());
        sourceDBConfObj.put("user_name", storeConnectionBean.getUser_name());
        sourceDBConfObj.put("database_pad", storeConnectionBean.getDatabase_pwd());
        sourceDBConfObj.put("database_type", storeConnectionBean.getDatabase_type());
        sourceDBConfObj.put("database_name", storeConnectionBean.getDatabase_name());
        sourceDBConfObj.put("signal_file_list", new ArrayList<>());
        Result collectTableResult = Dbo.queryResult("SELECT dbs.database_id, ti.table_id, ti.table_name, " + "ti.table_ch_name, ti.table_count, ti.source_tableid, ti.valid_s_date, ti.valid_e_date, ti.sql, " + "ti.remark, ti.is_user_defined,  case when tsi.is_zipper = ? then tsi.is_md5 else ti.is_md5 end," + "ti.is_register,ti.is_parallel,ti.page_sql,ti.rec_num_date," + "ti.unload_type,ti.is_customize_sql,ti.pageparallels, ti.dataincrement,tsi.storage_type, " + "tsi.storage_time, tsi.is_zipper, tsi.hyren_name as storage_table_name, ds.datasource_name, " + "ai.agent_name, ai.agent_id, ds.source_id, ai.user_id,tc.interval_time,tc.over_date," + "dsr.storage_date FROM " + DataSource.TableName + " ds " + " JOIN " + AgentInfo.TableName + " ai ON ds.source_id = ai.source_id" + " JOIN " + DatabaseSet.TableName + " dbs ON ai.agent_id = dbs.agent_id" + " LEFT JOIN " + TableInfo.TableName + " ti on ti.database_id = dbs.database_id" + " LEFT JOIN " + TableCycle.TableName + " tc on ti.table_id = tc.table_id" + " LEFT JOIN " + CollectJobClassify.TableName + " cjc on dbs.classify_id = cjc.classify_id" + " LEFT JOIN " + TableStorageInfo.TableName + " tsi on tsi.table_id = ti.table_id" + " LEFT JOIN " + DataStoreReg.TableName + " dsr on dsr.table_id = ti.table_id " + " WHERE dbs.database_id = ?", IsFlag.Fou.getCode(), colSetId);
        List<Map<String, Object>> collectTables = JsonUtil.toObject(collectTableResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
        });
        for (int i = 0; i < collectTables.size(); i++) {
            Map<String, Object> collectTable = collectTables.get(i);
            Long tableId = Long.parseLong(collectTable.get("table_id").toString());
            List<ColumnMerge> columnMerges = Dbo.queryList(ColumnMerge.class, "select * from " + ColumnMerge.TableName + " where table_id = ?", tableId);
            if (!columnMerges.isEmpty()) {
                collectTable.put("column_merge_list", columnMerges);
            } else {
                collectTable.put("column_merge_list", new ArrayList<>());
            }
            Result tableColResult = Dbo.queryResult("select tc.column_id, tc.is_primary_key, tc.column_name, tc.column_ch_name, " + " tc.valid_s_date, tc.valid_e_date, tc.is_get, tc.column_type, tc.tc_remark, tc.is_alive, " + " tc.is_new, tc.tc_or from " + TableColumn.TableName + " tc where tc.table_id = ? and tc.is_get = ?", tableId, IsFlag.Shi.getCode());
            List<Map<String, Object>> tableColArray = JsonUtil.toObject(tableColResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
            });
            collectTable.put("collectTableColumnBeanList", tableColArray);
            List<Map<String, Object>> tbcol_srctgt_maps = Dbo.queryList("select * from " + TbcolSrctgtMap.TableName + " where column_id in ( select column_id from " + TableColumn.TableName + " where table_id = ? and is_get = ?)", tableId, IsFlag.Shi.getCode());
            for (int j = 0; j < tableColArray.size(); j++) {
                Map<String, Object> tableColObj = tableColArray.get(j);
                Long columnId = Long.parseLong(tableColObj.get("column_id").toString());
                for (Map<String, Object> tbcol_srctgt_map : tbcol_srctgt_maps) {
                    if (tbcol_srctgt_map.get("column_id").equals(columnId)) {
                        tbcol_srctgt_map.put("column_name", tableColObj.get("column_name"));
                    }
                }
            }
            collectTable.put("tbColTarTypeMaps", tbcol_srctgt_maps);
            Result dataStoreResult = Dbo.queryResult("select dsl.dsl_id, dsl.dsl_name, dsl.store_type, dsl.is_hadoopclient " + " from " + DataStoreLayer.TableName + " dsl" + " where dsl.dsl_id in (select drt.dsl_id from " + DtabRelationStore.TableName + " drt where drt.tab_id = " + " (select storage_id from " + TableStorageInfo.TableName + " tsi where tsi.table_id = ?) AND drt.data_source = ?)", tableId, StoreLayerDataSource.DB.getCode());
            List<Map<String, Object>> dataStoreArray = JsonUtil.toObject(dataStoreResult.toJSON(), new TypeReference<List<Map<String, Object>>>() {
            });
            for (int m = 0; m < dataStoreArray.size(); m++) {
                Map<String, Object> dataStore = dataStoreArray.get(m);
                Long dslId = Long.parseLong(dataStore.get("dsl_id").toString());
                Result result = Dbo.queryResult("select storage_property_key, storage_property_val, is_file from " + DataStoreLayerAttr.TableName + " where dsl_id = ?", dslId);
                if (result.isEmpty()) {
                    throw new BusinessException("根据存储层配置ID" + dslId + "未获取到存储层配置属性信息");
                }
                Map<String, String> dataStoreConnectAttr = new HashMap<>();
                Map<String, String> dataStoreLayerFile = new HashMap<>();
                for (int n = 0; n < result.getRowCount(); n++) {
                    IsFlag fileFlag = IsFlag.ofEnumByCode(result.getString(n, "is_file"));
                    if (fileFlag == IsFlag.Shi) {
                        dataStoreLayerFile.put(result.getString(n, "storage_property_key"), result.getString(n, "storage_property_val"));
                    } else {
                        dataStoreConnectAttr.put(result.getString(n, "storage_property_key"), result.getString(n, "storage_property_val"));
                    }
                }
                dataStore.put("data_store_connect_attr", dataStoreConnectAttr);
                dataStore.put("data_store_layer_file", dataStoreLayerFile);
                List<Object> storeLayers = Dbo.queryOneColumnList("select dsla.dsla_storelayer from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla" + " on dsla.dslad_id = csi.dslad_id" + " where csi.col_id in" + " (select column_id from " + TableColumn.TableName + " where table_id = ?) " + " and dsla.dsl_id = ? AND csi.data_source = ?", tableId, dslId, StoreLayerDataSource.DB.getCode());
                Result columnResult = Dbo.queryResult("select dsla.dsla_storelayer, csi.csi_number, tc.column_name " + " from " + DcolRelationStore.TableName + " csi" + " left join " + DataStoreLayerAdded.TableName + " dsla" + " on dsla.dslad_id = csi.dslad_id" + " join " + TableColumn.TableName + " tc " + " on csi.col_id = tc.column_id" + " where csi.col_id in (select column_id from " + TableColumn.TableName + " where table_id = ?) and dsla.dsl_id = ? AND csi.data_source = ?", tableId, dslId, StoreLayerDataSource.DB.getCode());
                Map<String, Map<String, Integer>> additInfoFieldMap = new HashMap<>();
                if (!columnResult.isEmpty() && !storeLayers.isEmpty()) {
                    for (Object obj : storeLayers) {
                        Map<String, Integer> fieldMap = new HashMap<>();
                        String storeLayer = (String) obj;
                        for (int h = 0; h < columnResult.getRowCount(); h++) {
                            String dslaStoreLayer = columnResult.getString(h, "dsla_storelayer");
                            if (storeLayer.equals(dslaStoreLayer)) {
                                fieldMap.put(columnResult.getString(h, "column_name"), columnResult.getInteger(h, "csi_number"));
                            }
                        }
                        additInfoFieldMap.put(storeLayer, fieldMap);
                    }
                } else {
                    defaultPrimaryKey(tableId, dslId, additInfoFieldMap);
                }
                dataStore.put("additInfoFieldMap", additInfoFieldMap);
            }
            collectTable.put("dataStoreConfBean", dataStoreArray);
        }
        sourceDBConfObj.put("collectTableBeanArray", collectTables);
        String methodName = AgentActionUtil.SENDDBCOLLECTTASKINFO;
        if (StringUtil.isNotBlank(etl_date)) {
            methodName = AgentActionUtil.JDBCDIRECTEXECUTEIMMEDIATELY;
        }
        SendMsgUtil.sendDBCollectTaskInfo(Long.parseLong(sourceDBConfObj.get("database_id").toString()), Long.parseLong(sourceDBConfObj.get("agent_id").toString()), UserUtil.getUserId() == null ? user_id : UserUtil.getUserId(), JsonUtil.toJson(sourceDBConfObj), methodName, etl_date, "false", sqlParam);
    }
}
