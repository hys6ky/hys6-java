package hyren.serv6.b.batchcollection.dbAgentcollect.datatransfer;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.CheckParam;
import hyren.serv6.b.agent.tools.SendMsgUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.DataExtractType;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.entity.TableInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.AgentActionUtil;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Service
@Slf4j
@Api("数据转存配置管理")
@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-04-19 14:15")
public class DataTransferService {

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getInitDataTransfer(long colSetId) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("采集任务( %s ), 不存在", colSetId);
        }
        countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + TableInfo.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("采集任务( %s ), 不存在表信息", colSetId);
        }
        List<Map<String, Object>> dataBaseTransDataList = Dbo.queryList("  SELECT t1.database_id,t1.table_id,t1.table_name,t1.table_ch_name,t2.is_archived,t1.unload_type FROM " + TableInfo.TableName + " t1 " + " LEFT JOIN " + DataExtractionDef.TableName + " t2 ON t1.table_id = t2.table_id WHERE " + " t1.database_id = ? ORDER BY t1.table_name", colSetId);
        List<Map<String, Object>> xmlDataTransfer = getDataTransfer(colSetId);
        dataBaseTransDataList.forEach(databaseItemMap -> {
            Map<String, Object> extractionMap = Dbo.queryOneObject("SELECT t1.* FROM " + DataExtractionDef.TableName + " t1 JOIN " + TableInfo.TableName + " t2 ON t1.table_id = t2.table_id " + " WHERE t2.database_id = ? AND  t2.table_id = ?", colSetId, databaseItemMap.get("table_id"));
            xmlDataTransfer.forEach(itemMap -> {
                if (itemMap.get("table_name").equals(databaseItemMap.get("table_name"))) {
                    List<Map<String, Object>> storageList = JsonUtil.toObject(JsonUtil.toJson(itemMap.get("storage")), new TypeReference<List<Map<String, Object>>>() {
                    });
                    if (storageList.isEmpty()) {
                        CheckParam.throwErrorMsg("表(%s)下的抽取方式为空,请检查确认!!!", itemMap.get("table_name"));
                    }
                    if (!extractionMap.isEmpty()) {
                        Iterator<Map<String, Object>> iterator = storageList.iterator();
                        while (iterator.hasNext()) {
                            Map<String, Object> storageMap = iterator.next();
                            String storageFormat = String.valueOf(storageMap.get("dbfile_format"));
                            if (storageFormat.equals(extractionMap.get("dbfile_format"))) {
                                extractionMap.put("is_header", storageMap.get("is_header"));
                                extractionMap.put("database_separator", StringUtil.unicode2String(String.valueOf(storageMap.get("database_separator"))));
                                extractionMap.put("plane_url", storageMap.get("plane_url"));
                                extractionMap.put("database_code", storageMap.get("database_code"));
                                extractionMap.put("row_separator", String.valueOf(storageMap.get("row_separator")));
                                iterator.remove();
                                break;
                            }
                        }
                        storageList.add(0, extractionMap);
                        itemMap.put("storage", storageDataFormat(storageList));
                    } else {
                        itemMap.put("storage", storageDataFormat(storageList));
                    }
                    itemMap.put("table_name", databaseItemMap.get("table_name"));
                    itemMap.put("ded_id", extractionMap.get("ded_id"));
                    itemMap.put("table_ch_name", databaseItemMap.get("table_ch_name"));
                    itemMap.put("table_id", databaseItemMap.get("table_id"));
                    itemMap.put("unload_type", databaseItemMap.get("unload_type"));
                    itemMap.put("is_archived", databaseItemMap.get("is_archived"));
                }
            });
        });
        return xmlDataTransfer;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "storageData", desc = "", range = "")
    private List<Map<String, Object>> storageDataFormat(List<Map<String, Object>> storageData) {
        storageData.forEach(itemMap -> {
            if (StringUtil.isNotBlank(String.valueOf(itemMap.get("row_separator")))) {
                String separator = StringUtil.unicode2String(String.valueOf(itemMap.get("row_separator")));
                switch(separator) {
                    case "\r\n":
                        itemMap.put("row_separator", "\\r\\n");
                        break;
                    case "\n":
                        itemMap.put("row_separator", "\\n");
                        break;
                    case "\r":
                        itemMap.put("row_separator", "\\r");
                        break;
                    default:
                        itemMap.put("row_separator", separator);
                        break;
                }
            }
            if (StringUtil.isNotBlank(String.valueOf(itemMap.get("database_separatorr")))) {
                itemMap.put("database_separatorr", StringUtil.unicode2String(String.valueOf(itemMap.get("database_separatorr"))));
            } else {
                itemMap.put("database_separatorr", "");
            }
        });
        return storageData;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Map<String, Object>> getDataTransfer(long colSetId) {
        Map<String, Object> databaseInfo = getDatabaseSetInfo(colSetId);
        String respMsg = SendMsgUtil.getAllTableName((long) databaseInfo.get("agent_id"), getUserId(), databaseInfo, AgentActionUtil.GETALLTABLESTORAGE);
        return JsonUtil.toObject(respMsg, new TypeReference<List<Map<String, Object>>>() {
        });
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getDatabaseSetInfo(long colSetId) {
        long databaseNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (databaseNum == 0) {
            CheckParam.throwErrorMsg("任务(%s)不存在!!!", colSetId);
        }
        return Dbo.queryOneObject(" SELECT t1.dsl_id,  t1.fetch_size," + " t1.agent_id, t1.db_agent, t1.plane_url" + " FROM " + DatabaseSet.TableName + " t1" + " JOIN " + AgentInfo.TableName + " ai ON ai.agent_id = t1.agent_id" + " WHERE t1.database_id = ? and ai.user_id = ? AND ai.agent_type = ?", colSetId, getUserId(), AgentType.DBWenJian.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "dataExtractionDefs", desc = "", range = "", isBean = true, example = "")
    @Param(name = "tableInfos", desc = "", range = "", isBean = true, example = "", nullable = true)
    @Return(desc = "", range = "")
    public long saveDataTransferData(long colSetId, DataExtractionDef[] dataExtractionDefs, TableInfo[] tableInfos) {
        long countNum = Dbo.queryNumber("SELECT count(*) FROM " + DatabaseSet.TableName + " WHERE database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum == 0) {
            CheckParam.throwErrorMsg("采集任务ID(%s), 不存在", colSetId);
        }
        for (TableInfo tableInfo : tableInfos) {
            Dbo.execute("UPDATE " + TableInfo.TableName + "  SET table_ch_name = ? WHERE table_id = ? AND database_id = ?", tableInfo.getTable_ch_name(), tableInfo.getTable_id(), colSetId);
        }
        int index = 1;
        for (DataExtractionDef dataExtractionDef : dataExtractionDefs) {
            CheckParam.checkData("第(%s)张表,表关联的表ID不能为空", String.valueOf(dataExtractionDef.getTable_id()), index);
            CheckParam.checkData("第(%s)张表,是否需要表头不能为空", dataExtractionDef.getIs_header(), index);
            CheckParam.checkData("第(%s)张表,数据抽取落地编码不能为空", dataExtractionDef.getDatabase_code(), index);
            CheckParam.checkData("第(%s)张表,数据落地格式不能为空", dataExtractionDef.getDbfile_format(), index);
            CheckParam.checkData("第(%s)张表,数据是否转存不能为空", dataExtractionDef.getIs_archived(), index);
            CheckParam.checkData("第(%s)张表,是否有表头不能为空", dataExtractionDef.getIs_header(), index);
            FileFormat fileFormat = FileFormat.ofEnumByCode(dataExtractionDef.getDbfile_format());
            if (fileFormat == FileFormat.FeiDingChang || fileFormat == FileFormat.DingChang) {
                String row_separator = dataExtractionDef.getRow_separator();
                if (StringUtil.isNotBlank(row_separator)) {
                    dataExtractionDef.setRow_separator(StringUtil.string2Unicode(row_separator));
                } else {
                    CheckParam.throwErrorMsg("第(%s)张表,行分隔符不能为空", index);
                }
                String database_separatorr = dataExtractionDef.getDatabase_separatorr();
                if (StringUtil.isNotBlank(database_separatorr)) {
                    dataExtractionDef.setDatabase_separatorr(StringUtil.string2Unicode(database_separatorr));
                } else {
                    if (fileFormat != FileFormat.DingChang) {
                        CheckParam.throwErrorMsg("第(%s)张表,列分割符不能为空", index);
                    }
                }
            }
            dataExtractionDef.setData_extract_type(DataExtractType.YuanShuJuGeShi.getCode());
            if (dataExtractionDef.getDed_id() == null) {
                dataExtractionDef.setDed_id(PrimayKeyGener.getNextId());
                dataExtractionDef.add(Dbo.db());
            } else {
                dataExtractionDef.update(Dbo.db());
            }
            index++;
        }
        return colSetId;
    }
}
