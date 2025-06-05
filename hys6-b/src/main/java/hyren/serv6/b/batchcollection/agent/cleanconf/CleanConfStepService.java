package hyren.serv6.b.batchcollection.agent.cleanconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.agent.bean.ColumnCleanParam;
import hyren.serv6.b.agent.bean.TableCleanParam;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@Slf4j
@Api("配置清洗规则")
@DocClass(desc = "", author = "WangZhengcheng")
public class CleanConfStepService {

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getCleanConfInfo(long colSetId) {
        List<Object> tableIds = Dbo.queryOneColumnList("SELECT table_id FROM " + TableInfo.TableName + " WHERE database_id = ?", colSetId);
        if (tableIds.isEmpty()) {
            return new Result();
        }
        Map<String, Object> agentTypeMap = Dbo.queryOneObject("SELECT agent_type FROM " + AgentInfo.TableName + " t1 JOIN database_set t2 ON t1.agent_id = t2.agent_id  WHERE t2.database_id = ?", colSetId);
        if (agentTypeMap.get("agent_type").equals(AgentType.DBWenJian.getCode())) {
            StringBuilder strSB = new StringBuilder("SELECT ti.table_id, ti.table_name, ti.table_ch_name, " + " sum(CASE tc.clean_type WHEN ? THEN 1 ELSE 0 END) as compflag, " + " sum(CASE tc.clean_type WHEN ? THEN 1 ELSE 0 END) as replaceflag, " + " sum(CASE tc.clean_type WHEN ? THEN 1 ELSE 0 END) as trimflag " + " FROM " + TableInfo.TableName + " ti LEFT JOIN " + TableClean.TableName + " tc " + " ON ti.table_id = tc.table_id join data_extraction_def t5 on ti.table_id = t5.table_id " + " where ti.table_id in ( ");
            for (int i = 0; i < tableIds.size(); i++) {
                strSB.append(tableIds.get(i));
                if (i != tableIds.size() - 1)
                    strSB.append(",");
            }
            strSB.append(" ) AND t5.data_extract_type = ? AND  t5.is_archived = ? GROUP BY ti.table_id ORDER BY ti.table_name ");
            return Dbo.queryResult(strSB.toString(), CleanType.ZiFuBuQi.getCode(), CleanType.ZiFuTiHuan.getCode(), CleanType.ZiFuTrim.getCode(), DataExtractType.YuanShuJuGeShi.getCode(), IsFlag.Shi.getCode());
        } else {
            StringBuilder strSB = new StringBuilder("SELECT ti.table_id, ti.table_name, ti.table_ch_name, " + " sum(CASE tc.clean_type WHEN ? THEN 1 ELSE 0 END) as compflag, " + " sum(CASE tc.clean_type WHEN ? THEN 1 ELSE 0 END) as replaceflag, " + " sum(CASE tc.clean_type WHEN ? THEN 1 ELSE 0 END) as trimflag " + " FROM " + TableInfo.TableName + " ti LEFT JOIN " + TableClean.TableName + " tc " + " ON ti.table_id = tc.table_id " + " where ti.table_id in ( ");
            for (int i = 0; i < tableIds.size(); i++) {
                strSB.append(tableIds.get(i));
                if (i != tableIds.size() - 1)
                    strSB.append(",");
            }
            strSB.append(" ) GROUP BY ti.table_id ORDER BY ti.table_name");
            return Dbo.queryResult(strSB.toString(), CleanType.ZiFuBuQi.getCode(), CleanType.ZiFuTiHuan.getCode(), CleanType.ZiFuTrim.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "charCompletion", desc = "", range = "", isBean = true)
    public void saveSingleTbCompletionInfo(TableClean charCompletion) {
        if (StringUtil.isEmpty(charCompletion.getCharacter_filling())) {
            throw new BusinessException("保存整表字符补齐规则时，补齐字符不能为空");
        }
        if (charCompletion.getFilling_length() == null) {
            throw new BusinessException("保存整表字符补齐规则时，补齐长度不能为空");
        }
        if (StringUtil.isBlank(charCompletion.getFilling_type())) {
            throw new BusinessException("保存整表字符补齐规则时，必须选择补齐方式");
        }
        FillingType.ofEnumByCode(charCompletion.getFilling_type());
        if (charCompletion.getTable_id() == null) {
            throw new BusinessException("保存整表字符补齐规则是，必须关联表信息");
        }
        Dbo.execute("DELETE FROM " + TableClean.TableName + " WHERE table_id = ? AND clean_type = ?", charCompletion.getTable_id(), CleanType.ZiFuBuQi.getCode());
        charCompletion.setTable_clean_id(PrimayKeyGener.getNextId());
        charCompletion.setClean_type(CleanType.ZiFuBuQi.getCode());
        String characterFilling = charCompletion.getCharacter_filling();
        charCompletion.setCharacter_filling(StringUtil.string2Unicode(characterFilling));
        charCompletion.add(Dbo.db());
        List<Object> columnIds = getColumnIdByTableId(charCompletion.getTable_id());
        delColCleanByColIdAndType(columnIds, CleanType.ZiFuBuQi.getCode());
        for (Object columnId : columnIds) {
            ColumnClean columnClean = new ColumnClean();
            columnClean.setColumn_id((long) columnId);
            columnClean.setCharacter_filling(StringUtil.string2Unicode(characterFilling));
            columnClean.setFilling_length(charCompletion.getFilling_length());
            columnClean.setFilling_type(charCompletion.getFilling_type());
            columnClean.setClean_type(CleanType.ZiFuBuQi.getCode());
            columnClean.setCol_clean_id(PrimayKeyGener.getNextId());
            columnClean.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "charCompletion", desc = "", range = "", isBean = true)
    public void saveColCompletionInfo(ColumnClean charCompletion) {
        if (StringUtil.isEmpty(charCompletion.getCharacter_filling())) {
            throw new BusinessException("保存列字符补齐规则时，补齐字符不能为空");
        }
        if (charCompletion.getFilling_length() == null) {
            throw new BusinessException("保存列字符补齐规则时，补齐长度不能为空");
        }
        if (StringUtil.isBlank(charCompletion.getFilling_type())) {
            throw new BusinessException("保存列字符补齐规则时，必须选择补齐方式");
        }
        if (charCompletion.getColumn_id() == null) {
            throw new BusinessException("保存列字符补齐规则是，必须关联字段信息");
        }
        FillingType.ofEnumByCode(charCompletion.getFilling_type());
        Dbo.execute("DELETE FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", charCompletion.getColumn_id(), CleanType.ZiFuBuQi.getCode());
        charCompletion.setCol_clean_id(PrimayKeyGener.getNextId());
        charCompletion.setClean_type(CleanType.ZiFuBuQi.getCode());
        charCompletion.setCharacter_filling(StringUtil.string2Unicode(charCompletion.getCharacter_filling()));
        charCompletion.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getColCompletionInfo(long columnId) {
        Map<String, Object> compMap = Dbo.queryOneObject("select col_clean_id, filling_type, character_filling, filling_length, column_id" + " from " + ColumnClean.TableName + " where column_id = ? and clean_type = ?", columnId, CleanType.ZiFuBuQi.getCode());
        if (!compMap.isEmpty()) {
            compMap.put("character_filling", StringUtil.unicode2String((String) compMap.get("character_filling")));
            return compMap;
        }
        Map<String, Object> tbCompMap = Dbo.queryOneObject("SELECT tc.table_clean_id, tc.filling_type, tc.character_filling, tc.filling_length" + " FROM " + TableClean.TableName + " tc" + " WHERE tc.table_id = (SELECT table_id FROM " + TableColumn.TableName + " WHERE column_id = ?)" + " AND tc.clean_type = ?", columnId, CleanType.ZiFuBuQi.getCode());
        if (tbCompMap.isEmpty()) {
            return tbCompMap;
        }
        tbCompMap.put("character_filling", StringUtil.unicode2String((String) tbCompMap.get("character_filling")));
        return tbCompMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getTbCompletionInfo(long tableId) {
        Map<String, Object> tbCompMap = Dbo.queryOneObject("SELECT table_clean_id, filling_type, character_filling, " + "filling_length  FROM " + TableClean.TableName + " WHERE table_id = ? AND clean_type = ?", tableId, CleanType.ZiFuBuQi.getCode());
        if (tbCompMap.isEmpty()) {
            return tbCompMap;
        }
        tbCompMap.put("character_filling", StringUtil.unicode2String((String) tbCompMap.get("character_filling")));
        return tbCompMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "replaceString", desc = "", range = "")
    @Param(name = "tableId", desc = "", range = "")
    public void saveSingleTbReplaceInfo(String replaceString, long tableId) {
        List<TableClean> replaceList = JsonUtil.toObject(replaceString, new TypeReference<List<TableClean>>() {
        });
        Dbo.execute("DELETE FROM " + TableClean.TableName + " WHERE table_id = ? AND clean_type = ?", tableId, CleanType.ZiFuTiHuan.getCode());
        List<Object> columnIds = getColumnIdByTableId(tableId);
        delColCleanByColIdAndType(columnIds, CleanType.ZiFuTiHuan.getCode());
        if (replaceList != null && !replaceList.isEmpty()) {
            for (int i = 0; i < replaceList.size(); i++) {
                TableClean tableClean = replaceList.get(i);
                if (StringUtil.isEmpty(tableClean.getField())) {
                    throw new BusinessException("保存表字符替换规则时，第" + (i + 1) + "条数据缺少源字符串");
                }
                if (StringUtil.isEmpty(tableClean.getReplace_feild())) {
                    throw new BusinessException("保存表字符替换规则时，第" + (i + 1) + "条数据缺少替换字符串");
                }
                tableClean.setTable_clean_id(PrimayKeyGener.getNextId());
                tableClean.setClean_type(CleanType.ZiFuTiHuan.getCode());
                tableClean.setTable_id(tableId);
                String field = tableClean.getField();
                String replaceFeild = tableClean.getReplace_feild();
                tableClean.setField(StringUtil.string2Unicode(field));
                tableClean.setReplace_feild(StringUtil.string2Unicode(replaceFeild));
                tableClean.add(Dbo.db());
                for (Object columnId : columnIds) {
                    ColumnClean columnClean = new ColumnClean();
                    columnClean.setCol_clean_id(PrimayKeyGener.getNextId());
                    columnClean.setClean_type(CleanType.ZiFuTiHuan.getCode());
                    columnClean.setColumn_id((long) columnId);
                    columnClean.setField(StringUtil.string2Unicode(field));
                    columnClean.setReplace_feild(StringUtil.string2Unicode(replaceFeild));
                    columnClean.add(Dbo.db());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "replaceString", desc = "", range = "")
    @Param(name = "columnId", desc = "", range = "")
    public void saveColReplaceInfo(String replaceString, long columnId) {
        List<ColumnClean> replaceList = JsonUtil.toObject(replaceString, new TypeReference<List<ColumnClean>>() {
        });
        Dbo.execute("DELETE FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", columnId, CleanType.ZiFuTiHuan.getCode());
        if (replaceList != null && !replaceList.isEmpty()) {
            for (int i = 0; i < replaceList.size(); i++) {
                ColumnClean columnClean = replaceList.get(i);
                if (StringUtil.isEmpty(columnClean.getField())) {
                    throw new BusinessException("保存列字符替换规则时，第" + (i + 1) + "条数据缺少源字符串");
                }
                if (StringUtil.isEmpty(columnClean.getReplace_feild())) {
                    throw new BusinessException("保存列字符替换规则时，第" + (i + 1) + "条数据缺少替换字符串");
                }
                columnClean.setCol_clean_id(PrimayKeyGener.getNextId());
                columnClean.setClean_type(CleanType.ZiFuTiHuan.getCode());
                columnClean.setColumn_id(columnId);
                columnClean.setField(StringUtil.string2Unicode(columnClean.getField()));
                columnClean.setReplace_feild(StringUtil.string2Unicode(columnClean.getReplace_feild()));
                columnClean.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getSingleTbReplaceInfo(long tableId) {
        Result result = Dbo.queryResult("SELECT table_clean_id, field, replace_feild FROM " + TableClean.TableName + " WHERE table_id = ? AND clean_type = ?", tableId, CleanType.ZiFuTiHuan.getCode());
        if (result.isEmpty()) {
            return result;
        }
        result.setObject(0, "field", StringUtil.unicode2String(result.getString(0, "field")));
        result.setObject(0, "replace_feild", StringUtil.unicode2String(result.getString(0, "replace_feild")));
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getColReplaceInfo(long columnId) {
        Result columnResult = Dbo.queryResult("select col_clean_id, field, replace_feild, column_id" + " from " + ColumnClean.TableName + " where column_id = ? and clean_type = ?", columnId, CleanType.ZiFuTiHuan.getCode());
        if (!columnResult.isEmpty()) {
            columnResult.setObject(0, "field", StringUtil.unicode2String(columnResult.getString(0, "field")));
            columnResult.setObject(0, "replace_feild", StringUtil.unicode2String(columnResult.getString(0, "replace_feild")));
            return columnResult;
        }
        Result tableResult = Dbo.queryResult("SELECT tc.table_clean_id, tc.field, tc.replace_feild " + " FROM " + TableClean.TableName + " tc" + " WHERE tc.table_id = (SELECT table_id FROM " + TableColumn.TableName + " WHERE column_id = ?" + " AND tc.clean_type = ?)", columnId, CleanType.ZiFuTiHuan.getCode());
        if (tableResult.isEmpty()) {
            return tableResult;
        }
        tableResult.setObject(0, "field", StringUtil.unicode2String(tableResult.getString(0, "field")));
        tableResult.setObject(0, "replace_feild", StringUtil.unicode2String(tableResult.getString(0, "replace_feild")));
        return tableResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getColumnInfo(long tableId) {
        List<Object> columnIds = Dbo.queryOneColumnList("select column_id from " + TableColumn.TableName + " where table_id = ? and is_get = ? and is_new = ?", tableId, IsFlag.Shi.getCode(), IsFlag.Fou.getCode());
        if (columnIds.isEmpty()) {
            return new Result();
        }
        StringBuilder sqlSB = new StringBuilder("SELECT t1.column_id,t1.column_name,t1.column_ch_name," + " t2.table_name," + " sum(case t3.clean_type when ? then 1 else 0 end) as compflag, " + " sum(case t3.clean_type when ? then 1 else 0 end) as replaceflag, " + " sum(case t3.clean_type when ? then 1 else 0 end ) as formatflag, " + " sum(case t3.clean_type when ? then 1 else 0 end) as splitflag, " + " sum(case t3.clean_type when ? then 1 else 0 end) as codevalueflag, " + " sum(case t3.clean_type when ? then 1 else 0 end) as trimflag " + " FROM " + TableColumn.TableName + " t1 JOIN " + TableInfo.TableName + " t2 ON t1.table_id = t2.table_id " + " left join " + ColumnClean.TableName + " t3 on t1.column_id = t3.column_id " + " WHERE t1.column_id in ( ");
        for (int i = 0; i < columnIds.size(); i++) {
            sqlSB.append(columnIds.get(i));
            if (i != columnIds.size() - 1)
                sqlSB.append(",");
        }
        sqlSB.append(" ) GROUP BY t1.column_id, t2.table_name order by cast(t1.tc_remark as integer) asc ");
        return Dbo.queryResult(sqlSB.toString(), CleanType.ZiFuBuQi.getCode(), CleanType.ZiFuTiHuan.getCode(), CleanType.ShiJianZhuanHuan.getCode(), CleanType.ZiFuChaiFen.getCode(), CleanType.MaZhiZhuanHuan.getCode(), CleanType.ZiFuTrim.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "compFlag", desc = "", range = "")
    @Param(name = "replaceFlag", desc = "", range = "")
    @Param(name = "compType", desc = "", range = "", nullable = true, valueIfNull = "0")
    @Param(name = "compChar", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "compLen", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "oriFieldArr", desc = "", range = "", nullable = true, valueIfNull = "")
    @Param(name = "replaceFeildArr", desc = "", range = "", nullable = true, valueIfNull = "")
    public void saveAllTbCleanConfigInfo(long colSetId, String compFlag, String replaceFlag, String compType, String compChar, String compLen, String[] oriFieldArr, String[] replaceFeildArr) {
        Dbo.execute("DELETE FROM clean_parameter WHERE database_id = ?", colSetId);
        List<Object> tableIds = getTableIdByColSetId(colSetId);
        List<Object> columnIds = getColumnIdByColSetId(colSetId);
        delTbCleanByTbIdAndType(tableIds, CleanType.ZiFuBuQi.getCode());
        delColCleanByColIdAndType(columnIds, CleanType.ZiFuBuQi.getCode());
        delTbCleanByTbIdAndType(tableIds, CleanType.ZiFuTiHuan.getCode());
        delColCleanByColIdAndType(columnIds, CleanType.ZiFuTiHuan.getCode());
        if (IsFlag.ofEnumByCode(compFlag) == IsFlag.Shi) {
            FillingType fillingType = FillingType.ofEnumByCode(compType);
            CleanParameter allTbClean = new CleanParameter();
            allTbClean.setC_id(PrimayKeyGener.getNextId());
            allTbClean.setDatabase_id(colSetId);
            allTbClean.setClean_type(CleanType.ZiFuBuQi.getCode());
            allTbClean.setFilling_type(fillingType.getCode());
            allTbClean.setCharacter_filling(StringUtil.string2Unicode(compChar));
            try {
                allTbClean.setFilling_length(Long.valueOf(compLen));
            } catch (NumberFormatException e) {
                e.printStackTrace();
                throw new BusinessException("filling length must be number .");
            }
            allTbClean.add(Dbo.db());
            for (Object tableId : tableIds) {
                TableClean tableClean = new TableClean();
                tableClean.setTable_clean_id(PrimayKeyGener.getNextId());
                tableClean.setTable_id((long) tableId);
                tableClean.setCharacter_filling(StringUtil.string2Unicode(compChar));
                tableClean.setFilling_length(Long.valueOf(compLen));
                tableClean.setFilling_type(compType);
                tableClean.setClean_type(CleanType.ZiFuBuQi.getCode());
                tableClean.add(Dbo.db());
            }
            for (Object columnId : columnIds) {
                ColumnClean columnClean = new ColumnClean();
                columnClean.setColumn_id((long) columnId);
                columnClean.setCharacter_filling(StringUtil.string2Unicode(compChar));
                columnClean.setFilling_length(Long.valueOf(compLen));
                columnClean.setFilling_type(compType);
                columnClean.setClean_type(CleanType.ZiFuBuQi.getCode());
                columnClean.setCol_clean_id(PrimayKeyGener.getNextId());
                columnClean.add(Dbo.db());
            }
        }
        if (IsFlag.ofEnumByCode(replaceFlag) == IsFlag.Shi) {
            if (!(oriFieldArr.length > 0)) {
                throw new BusinessException("保存所有表字符替换清洗设置时，缺失原字符");
            }
            if (!(replaceFeildArr.length > 0)) {
                throw new BusinessException("保存所有表字符替换清洗设置时，缺失替换字符");
            }
            for (int i = 0; i < oriFieldArr.length; i++) {
                String oriField = oriFieldArr[i];
                String replaceFeild = replaceFeildArr[i];
                if (StringUtil.isEmpty(oriField)) {
                    throw new BusinessException("保存所有表字符替换清洗时，请填写第" + (i + 1) + "条数据的原字符");
                }
                if (StringUtil.isEmpty(replaceFeild)) {
                    throw new BusinessException("保存所有表字符替换清洗时，请填写第" + (i + 1) + "条数据的替换字符");
                }
                CleanParameter allTbClean = new CleanParameter();
                allTbClean.setC_id(PrimayKeyGener.getNextId());
                allTbClean.setDatabase_id(colSetId);
                allTbClean.setClean_type(CleanType.ZiFuTiHuan.getCode());
                allTbClean.setField(StringUtil.string2Unicode(oriField));
                allTbClean.setReplace_feild(StringUtil.string2Unicode(replaceFeild));
                allTbClean.add(Dbo.db());
                for (Object tableId : tableIds) {
                    TableClean tableClean = new TableClean();
                    tableClean.setTable_id((long) tableId);
                    tableClean.setTable_clean_id(PrimayKeyGener.getNextId());
                    tableClean.setClean_type(CleanType.ZiFuTiHuan.getCode());
                    tableClean.setField(StringUtil.string2Unicode(oriField));
                    tableClean.setReplace_feild(StringUtil.string2Unicode(replaceFeild));
                    tableClean.add(Dbo.db());
                }
                for (Object columnId : columnIds) {
                    ColumnClean columnClean = new ColumnClean();
                    columnClean.setCol_clean_id(PrimayKeyGener.getNextId());
                    columnClean.setClean_type(CleanType.ZiFuTiHuan.getCode());
                    columnClean.setColumn_id((long) columnId);
                    columnClean.setField(StringUtil.string2Unicode(oriField));
                    columnClean.setReplace_feild(StringUtil.string2Unicode(replaceFeild));
                    columnClean.add(Dbo.db());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getAllTbCleanReplaceInfo(long colSetId) {
        Result replaceResult = Dbo.queryResult("SELECT c_id, field, replace_feild FROM " + CleanParameter.TableName + " WHERE database_id = ? AND clean_type = ?", colSetId, CleanType.ZiFuTiHuan.getCode());
        if (!replaceResult.isEmpty()) {
            for (int i = 0; i < replaceResult.getRowCount(); i++) {
                replaceResult.setObject(i, "field", StringUtil.unicode2String(replaceResult.getString(i, "field")));
                replaceResult.setObject(i, "replace_feild", StringUtil.unicode2String(replaceResult.getString(i, "replace_feild")));
            }
        }
        return replaceResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getAllTbCleanCompInfo(long colSetId) {
        Result compResult = Dbo.queryResult("SELECT c_id, filling_type, character_filling, filling_length " + " FROM " + CleanParameter.TableName + " WHERE database_id = ? AND clean_type = ?", colSetId, CleanType.ZiFuBuQi.getCode());
        if (compResult.isEmpty()) {
            return compResult;
        }
        if (compResult.getRowCount() > 1) {
            throw new BusinessException("对所有表设置的字符补齐规则不唯一");
        }
        compResult.setObject(0, "character_filling", StringUtil.unicode2String(compResult.getString(0, "character_filling")));
        return compResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getDateFormatInfo(long columnId) {
        return Dbo.queryResult("select col_clean_id, old_format, convert_format FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", columnId, CleanType.ShiJianZhuanHuan.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dateFormat", desc = "", range = "", isBean = true)
    public void saveDateFormatInfo(ColumnClean dateFormat) {
        if (StringUtil.isBlank(dateFormat.getOld_format())) {
            throw new BusinessException("请填写原日期格式");
        }
        if (StringUtil.isBlank(dateFormat.getConvert_format())) {
            throw new BusinessException("请填写转换后日期格式");
        }
        if (dateFormat.getColumn_id() == null) {
            throw new BusinessException("保存日期转换信息必须关联字段");
        }
        Dbo.execute("DELETE FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", dateFormat.getColumn_id(), CleanType.ShiJianZhuanHuan.getCode());
        dateFormat.setCol_clean_id(PrimayKeyGener.getNextId());
        dateFormat.setClean_type(CleanType.ShiJianZhuanHuan.getCode());
        dateFormat.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getColSplitInfo(long columnId) {
        Result result = Dbo.queryResult("select * from " + ColumnSplit.TableName + " WHERE column_id = ?", columnId);
        if (result.isEmpty()) {
            return result;
        }
        for (int i = 0; i < result.getRowCount(); i++) {
            result.setObject(i, "split_sep", StringUtil.unicode2String(result.getString(i, "split_sep")));
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSplitId", desc = "", range = "")
    @Param(name = "colCleanId", desc = "", range = "")
    public void deleteColSplitInfo(long colSplitId, long colCleanId) {
        Dbo.execute("delete from " + TableColumn.TableName + " where column_name in (select t1.column_name from " + TableColumn.TableName + " t1 " + " JOIN " + ColumnSplit.TableName + " t2 ON t1.column_name = t2.col_name " + " JOIN " + ColumnClean.TableName + " t3 ON t2.col_clean_id = t3.col_clean_id " + " WHERE t2.col_clean_id = ? and  t2.col_split_id = ? and t1.is_new = ?)", colCleanId, colSplitId, IsFlag.Shi.getCode());
        DboExecute.deletesOrThrow("列拆分规则删除失败", "delete from " + ColumnSplit.TableName + " where col_split_id = ?", colSplitId);
        long splitCount = Dbo.queryNumber("select count(1) from column_split where col_clean_id = ?", colCleanId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (splitCount == 0) {
            DboExecute.deletesOrThrow("列拆分规则删除失败", "delete from " + ColumnClean.TableName + " where col_clean_id = ? and clean_type = ?", colCleanId, CleanType.ZiFuChaiFen.getCode());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnClean", desc = "", range = "", isBean = true)
    @Param(name = "columnSplitString", desc = "", range = "")
    @Param(name = "tableId", desc = "", range = "")
    public void saveColSplitInfo(ColumnClean columnClean, String columnSplitString, long tableId) {
        if (columnClean.getColumn_id() == null) {
            throw new BusinessException("保存列拆分时必须关联字段");
        }
        if (columnClean.getCol_clean_id() != null) {
            columnClean.setClean_type(CleanType.ZiFuChaiFen.getCode());
            columnClean.update(Dbo.db());
            Dbo.execute("delete from " + TableColumn.TableName + " where column_name in " + " (select t1.column_name from " + TableColumn.TableName + " t1 " + " JOIN " + ColumnSplit.TableName + " t2 ON t1.column_name = t2.col_name " + " JOIN " + ColumnClean.TableName + " t3 ON t2.col_clean_id = t3.col_clean_id " + " WHERE t2.col_clean_id = ? and t2.column_id = ? and t1.table_id = ? and t1.is_new = ?)", columnClean.getCol_clean_id(), columnClean.getColumn_id(), tableId, IsFlag.Shi.getCode());
            Dbo.execute("delete from " + ColumnSplit.TableName + " where column_id = ?", columnClean.getColumn_id());
        } else {
            columnClean.setCol_clean_id(PrimayKeyGener.getNextId());
            columnClean.setClean_type(CleanType.ZiFuChaiFen.getCode());
            columnClean.add(Dbo.db());
        }
        List<ColumnSplit> columnSplits = JsonUtil.toObject(columnSplitString, new TypeReference<List<ColumnSplit>>() {
        });
        if (columnSplits != null && !columnSplits.isEmpty()) {
            for (int i = 0; i < columnSplits.size(); i++) {
                ColumnSplit columnSplit = columnSplits.get(i);
                if (StringUtil.isBlank(columnSplit.getSplit_type())) {
                    throw new BusinessException("保存字符拆分信息时，第" + (i + 1) + "条数据拆分方式不能为空");
                }
                CharSplitType charSplitType = CharSplitType.ofEnumByCode(columnSplit.getSplit_type());
                if (charSplitType == CharSplitType.ZhiDingFuHao) {
                    if (StringUtil.isBlank(columnSplit.getSplit_sep())) {
                        throw new BusinessException("按照自定符号进行拆分，第" + (i + 1) + "条数据必须填写自定义符号");
                    }
                    if (columnSplit.getSeq() == null) {
                        throw new BusinessException("按照自定符号进行拆分，第" + (i + 1) + "条数据必须填写值位置");
                    }
                } else if (charSplitType == CharSplitType.PianYiLiang) {
                    if (StringUtil.isBlank(columnSplit.getCol_offset())) {
                        throw new BusinessException("按照偏移量进行拆分，第" + (i + 1) + "条数据必须填写字段偏移量");
                    }
                } else {
                    throw new BusinessException("第" + (i + 1) + "条数据拆分方式错误");
                }
                columnSplit.setCol_split_id(PrimayKeyGener.getNextId());
                columnSplit.setColumn_id(columnClean.getColumn_id());
                columnSplit.setCol_clean_id(columnClean.getCol_clean_id());
                columnSplit.setValid_s_date(DateUtil.getSysDate());
                columnSplit.setValid_e_date(Constant._MAX_DATE_8);
                if (charSplitType == CharSplitType.ZhiDingFuHao) {
                    columnSplit.setSplit_sep(StringUtil.string2Unicode(columnSplit.getSplit_sep()));
                }
                columnSplit.add(Dbo.db());
                TableColumn tableColumn = new TableColumn();
                tableColumn.setTable_id(tableId);
                tableColumn.setIs_new(IsFlag.Shi.getCode());
                tableColumn.setIs_alive(IsFlag.Shi.getCode());
                tableColumn.setColumn_id(PrimayKeyGener.getNextId());
                tableColumn.setIs_primary_key(IsFlag.Fou.getCode());
                tableColumn.setColumn_name(columnSplit.getCol_name());
                tableColumn.setColumn_type(columnSplit.getCol_type());
                tableColumn.setColumn_ch_name(columnSplit.getCol_zhname());
                tableColumn.setValid_s_date(DateUtil.getSysDate());
                tableColumn.setValid_e_date(Constant._MAX_DATE_8);
                tableColumn.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getCVConversionInfo(long columnId) {
        return Dbo.queryResult("select osi.orig_sys_code, osi.orig_sys_name ||'('||osi.orig_sys_code||')' " + " as orig_sys_name, cc.codename as code_classify " + " from " + ColumnClean.TableName + " cc left join " + OrigSysoInfo.TableName + " osi" + " on cc.codesys = osi.orig_sys_code where cc.column_id = ? and clean_type = ?", columnId, CleanType.MaZhiZhuanHuan.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<OrigSysoInfo> getSysCVInfo() {
        return Dbo.queryList(OrigSysoInfo.class, "select * from " + OrigSysoInfo.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "origSysCode", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getCVClassifyBySysCode(String origSysCode) {
        return Dbo.queryResult("select code_classify,orig_sys_code from " + OrigCodeInfo.TableName + " where orig_sys_code = ? group by code_classify,orig_sys_code", origSysCode);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "codeClassify", desc = "", range = "")
    @Param(name = "origSysCode", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getCVInfo(String codeClassify, String origSysCode) {
        return Dbo.queryResult("select code_value, orig_value from " + OrigCodeInfo.TableName + "" + " where code_classify = ? and orig_sys_code = ? group by code_value, orig_value", codeClassify, origSysCode);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnClean", desc = "", range = "", isBean = true)
    public void saveCVConversionInfo(ColumnClean columnClean) {
        if (StringUtil.isBlank(columnClean.getCodename())) {
            throw new BusinessException("请选择码值系统类型");
        }
        if (StringUtil.isBlank(columnClean.getCodesys())) {
            throw new BusinessException("请选择码值系统名称");
        }
        if (columnClean.getColumn_id() == null) {
            throw new BusinessException("保存码值转换，必须关联字段");
        }
        Dbo.execute("DELETE FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", columnClean.getColumn_id(), CleanType.MaZhiZhuanHuan.getCode());
        columnClean.setCol_clean_id(PrimayKeyGener.getNextId());
        columnClean.setClean_type(CleanType.MaZhiZhuanHuan.getCode());
        columnClean.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getColMergeInfo(long tableId) {
        return Dbo.queryResult("select * from " + ColumnMerge.TableName + " where table_id = ?", tableId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnMergeString", desc = "", range = "")
    @Param(name = "tableId", desc = "", range = "")
    public void saveColMergeInfo(String columnMergeString, long tableId) {
        Dbo.execute("delete from " + TableColumn.TableName + " where column_name in " + " (select t1.column_name from " + TableColumn.TableName + " t1 " + " JOIN " + ColumnMerge.TableName + " t2 ON t1.table_id=t2.table_id " + " and t1.column_name = t2.col_name " + " where t2.table_id = ? and t1.is_new = ? )", tableId, IsFlag.Shi.getCode());
        Dbo.execute("delete from " + ColumnMerge.TableName + " where table_id = ?", tableId);
        List<ColumnMerge> columnMerges = JsonUtil.toObject(columnMergeString, new TypeReference<List<ColumnMerge>>() {
        });
        if (columnMerges != null && !columnMerges.isEmpty()) {
            for (int i = 0; i < columnMerges.size(); i++) {
                ColumnMerge columnMerge = columnMerges.get(i);
                if (StringUtil.isBlank(columnMerge.getOld_name())) {
                    throw new BusinessException("保存列合并时，第" + (i + 1) + "条数据必须选择要合并的字段");
                }
                if (StringUtil.isBlank(columnMerge.getCol_name())) {
                    throw new BusinessException("保存列合并时，第" + (i + 1) + "条数据必须填写合并后字段名称");
                }
                if (StringUtil.isBlank(columnMerge.getCol_type())) {
                    throw new BusinessException("保存列合并时，第" + (i + 1) + "条数据必须填写字段类型");
                }
                columnMerge.setTable_id(tableId);
                columnMerge.setCol_merge_id(PrimayKeyGener.getNextId());
                columnMerge.setValid_s_date(DateUtil.getSysDate());
                columnMerge.setValid_e_date(Constant._MAX_DATE_8);
                columnMerge.add(Dbo.db());
                TableColumn tableColumn = new TableColumn();
                tableColumn.setTable_id(tableId);
                tableColumn.setIs_new(IsFlag.Shi.getCode());
                tableColumn.setIs_alive(IsFlag.Shi.getCode());
                tableColumn.setColumn_id(PrimayKeyGener.getNextId());
                tableColumn.setIs_primary_key(IsFlag.Fou.getCode());
                tableColumn.setColumn_name(columnMerge.getCol_name());
                tableColumn.setColumn_type(columnMerge.getCol_type());
                tableColumn.setColumn_ch_name(columnMerge.getCol_zhname());
                tableColumn.setValid_s_date(DateUtil.getSysDate());
                tableColumn.setValid_e_date(Constant._MAX_DATE_8);
                tableColumn.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colMergeId", desc = "", range = "")
    public void deleteColMergeInfo(long colMergeId) {
        DboExecute.deletesOrThrow("删除列合并失败", "delete from " + TableColumn.TableName + " where column_name = " + " (select t1.column_name " + " from " + TableColumn.TableName + " t1 " + " JOIN " + ColumnMerge.TableName + " t2 ON t1.table_id = t2.table_id " + " and t1.column_name = t2.col_name " + " where t2.col_merge_id = ?)", colMergeId);
        DboExecute.deletesOrThrow("删除列合并失败", "delete from " + ColumnMerge.TableName + " where col_merge_id = ?", colMergeId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "sort", desc = "", range = "")
    public void saveAllTbCleanOrder(long colSetId, String sort) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未能找到数据库采集任务");
        }
        DboExecute.updatesOrThrow("保存全表清洗优先级失败", "update " + DatabaseSet.TableName + " set cp_or = ? where database_id = ?", sort, colSetId);
        updateCleanOrderByTbId(getTableIdByColSetId(colSetId), sort);
        updateCleanOrderByColId(getColumnIdByColSetId(colSetId), sort);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAllTbCleanOrder(long colSetId) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未能找到数据库采集任务");
        }
        List<Object> list = Dbo.queryOneColumnList("select cp_or from " + DatabaseSet.TableName + " where database_id = ?", colSetId);
        Object cp_or = list.get(0);
        if (null == cp_or || StringUtil.isBlank(cp_or.toString())) {
            return Collections.emptyList();
        }
        return cleanOrderFormat((String) cp_or);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Param(name = "sort", desc = "", range = "")
    public void saveSingleTbCleanOrder(long tableId, String sort) {
        DboExecute.updatesOrThrow("保存整表清洗优先级失败", "update " + TableInfo.TableName + " set ti_or = ? where table_id = ?", sort, tableId);
        updateCleanOrderByColId(getColumnIdByTableId(tableId), sort);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getSingleTbCleanOrder(long tableId, long colSetId) {
        long count = Dbo.queryNumber("select count(1) from " + TableInfo.TableName + " where table_id = ? and " + "database_id = ?", tableId, colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("在当前数据库采集任务中未找到该采集表");
        }
        List<Object> list = Dbo.queryOneColumnList("select ti_or from " + TableInfo.TableName + " where table_id = ?", tableId);
        if (list.isEmpty()) {
            return Collections.emptyList();
        }
        return cleanOrderFormat((String) list.get(0));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    @Param(name = "sort", desc = "", range = "")
    public void saveColCleanOrder(long columnId, String sort) {
        DboExecute.updatesOrThrow("保存列清洗优先级失败", "update " + TableColumn.TableName + " set tc_or = ? where column_id = ?", sort, columnId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnId", desc = "", range = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getColCleanOrder(long columnId, long tableId) {
        long count = Dbo.queryNumber("select count(1) from " + TableColumn.TableName + " where column_id = ? " + "and table_id = ?", columnId, tableId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到字段");
        }
        List<Object> list = Dbo.queryOneColumnList("select tc_or from " + TableColumn.TableName + " where column_id = ?", columnId);
        if (list.isEmpty()) {
            return Collections.emptyList();
        }
        return cleanOrderFormat((String) list.get(0));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colCleanString", desc = "", range = "")
    public void saveColCleanConfig(List<ColumnCleanParam> columnCleanParams) {
        if (columnCleanParams == null || columnCleanParams.isEmpty()) {
            throw new BusinessException("未获取到列清洗信息");
        }
        for (ColumnCleanParam param : columnCleanParams) {
            if (!param.isComplementFlag()) {
                Dbo.execute("DELETE FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", param.getColumnId(), CleanType.ZiFuBuQi.getCode());
            }
            if (!param.isReplaceFlag()) {
                Dbo.execute("DELETE FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", param.getColumnId(), CleanType.ZiFuTiHuan.getCode());
            }
            if (!param.isFormatFlag()) {
                Dbo.execute("DELETE FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", param.getColumnId(), CleanType.ShiJianZhuanHuan.getCode());
            }
            if (!param.isConversionFlag()) {
                Dbo.execute("DELETE FROM " + ColumnClean.TableName + " WHERE column_id = ? AND clean_type = ?", param.getColumnId(), CleanType.MaZhiZhuanHuan.getCode());
            }
            if (!param.isSpiltFlag()) {
                Result colSplitInfo = getColSplitInfo(param.getColumnId());
                if (!colSplitInfo.isEmpty()) {
                    for (int i = 0; i < colSplitInfo.getRowCount(); i++) {
                        deleteColSplitInfo(colSplitInfo.getLong(i, "col_split_id"), colSplitInfo.getLong(i, "col_clean_id"));
                    }
                }
            }
            if (param.isTrimFlag()) {
                Dbo.execute("delete from " + ColumnClean.TableName + " where column_id = ? and clean_type = ?", param.getColumnId(), CleanType.ZiFuTrim.getCode());
                ColumnClean trim = new ColumnClean();
                trim.setCol_clean_id(PrimayKeyGener.getNextId());
                trim.setClean_type(CleanType.ZiFuTrim.getCode());
                trim.setColumn_id(param.getColumnId());
                trim.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Param(name = "tbCleanString", desc = "", range = "")
    @Return(desc = "", range = "")
    public long saveDataCleanConfig(long colSetId, String tbCleanString) {
        List<TableCleanParam> tableCleanParams = JsonUtil.toObject(tbCleanString, new TypeReference<List<TableCleanParam>>() {
        });
        if (tableCleanParams == null || tableCleanParams.isEmpty()) {
            throw new BusinessException("未获取到表清洗信息");
        }
        for (TableCleanParam param : tableCleanParams) {
            if (!param.isComplementFlag()) {
                Result tbCompResult = Dbo.queryResult("select filling_type, character_filling, filling_length from " + TableClean.TableName + " where table_id = ? and clean_type = ?", param.getTableId(), CleanType.ZiFuBuQi.getCode());
                List<Object> columnIds = getColumnIdByTableId(param.getTableId());
                Dbo.execute("DELETE FROM " + TableClean.TableName + " WHERE table_id = ? AND clean_type = ?", param.getTableId(), CleanType.ZiFuBuQi.getCode());
                if (tbCompResult.getRowCount() == 1) {
                    StringBuilder strSBCol = new StringBuilder("delete from " + ColumnClean.TableName + " where column_id in ( ");
                    for (int j = 0; j < columnIds.size(); j++) {
                        strSBCol.append((long) columnIds.get(j));
                        if (j != columnIds.size() - 1)
                            strSBCol.append(",");
                    }
                    strSBCol.append(" ) and clean_type = ? and filling_type = ? and filling_length = ? and character_filling = ?");
                    Dbo.execute(strSBCol.toString(), CleanType.ZiFuBuQi.getCode(), tbCompResult.getString(0, "filling_type"), tbCompResult.getLong(0, "filling_length"), tbCompResult.getString(0, "character_filling"));
                }
            }
            if (!param.isReplaceFlag()) {
                Result tbReplResult = Dbo.queryResult("select field, replace_feild from " + TableClean.TableName + " where table_id = ? and clean_type = ?", param.getTableId(), CleanType.ZiFuTiHuan.getCode());
                List<Object> columnIds = getColumnIdByTableId(param.getTableId());
                Dbo.execute("DELETE FROM " + TableClean.TableName + " WHERE table_id = ? AND clean_type = ?", param.getTableId(), CleanType.ZiFuTiHuan.getCode());
                if (tbReplResult.getRowCount() > 0) {
                    for (int i = 0; i < tbReplResult.getRowCount(); i++) {
                        StringBuilder strSBCol = new StringBuilder("delete from " + ColumnClean.TableName + " where column_id in ( ");
                        for (int j = 0; j < columnIds.size(); j++) {
                            strSBCol.append((long) columnIds.get(j));
                            if (j != columnIds.size() - 1)
                                strSBCol.append(",");
                        }
                        strSBCol.append(" ) and clean_type = ? and field = ? and replace_feild = ?");
                        Dbo.execute(strSBCol.toString(), CleanType.ZiFuTiHuan.getCode(), tbReplResult.getString(i, "field"), tbReplResult.getString(i, "replace_feild"));
                    }
                }
            }
            if (param.isTrimFlag()) {
                Dbo.execute("delete from " + TableClean.TableName + " where table_id = ? and clean_type = ?", param.getTableId(), CleanType.ZiFuTrim.getCode());
                List<Object> columnIds = getColumnIdByTableId(param.getTableId());
                delColCleanByColIdAndType(columnIds, CleanType.ZiFuTrim.getCode());
                TableClean trim = new TableClean();
                trim.setTable_clean_id(PrimayKeyGener.getNextId());
                trim.setClean_type(CleanType.ZiFuTrim.getCode());
                trim.setTable_id(param.getTableId());
                trim.add(Dbo.db());
                for (Object columnId : columnIds) {
                    ColumnClean colTrim = new ColumnClean();
                    colTrim.setCol_clean_id(PrimayKeyGener.getNextId());
                    colTrim.setClean_type(CleanType.ZiFuTrim.getCode());
                    colTrim.setColumn_id((long) columnId);
                    colTrim.add(Dbo.db());
                }
            }
            if (!param.isTrimFlag()) {
                List<Object> columnIds = getColumnIdByTableId(param.getTableId());
                Dbo.execute("delete from " + TableClean.TableName + " where table_id = ? and clean_type = ?", param.getTableId(), CleanType.ZiFuTrim.getCode());
                StringBuilder strSBCol = new StringBuilder("delete from " + ColumnClean.TableName + " where column_id in ( ");
                for (int j = 0; j < columnIds.size(); j++) {
                    strSBCol.append((long) columnIds.get(j));
                    if (j != columnIds.size() - 1)
                        strSBCol.append(",");
                }
                strSBCol.append(" ) and clean_type = ?");
                Dbo.execute(strSBCol.toString(), CleanType.ZiFuTrim.getCode());
            }
        }
        return colSetId;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "order", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Map<String, Object>> cleanOrderFormat(String order) {
        List<Map<String, Object>> returnList = new ArrayList<>();
        Map<String, Object> json = JsonUtil.toObject(order, new TypeReference<LinkedHashMap<String, Object>>() {
        });
        Set<String> keys = json.keySet();
        for (String key : keys) {
            Map<String, Object> map = new HashMap<>();
            if (key.equals(CleanType.ZiFuBuQi.getCode())) {
                map.put("code", CleanType.ZiFuBuQi.getCode());
                map.put("order", json.get(CleanType.ZiFuBuQi.getCode()));
                returnList.add(map);
            } else if (key.equals(CleanType.ZiFuTiHuan.getCode())) {
                map.put("code", CleanType.ZiFuTiHuan.getCode());
                map.put("order", json.get(CleanType.ZiFuTiHuan.getCode()));
                returnList.add(map);
            } else if (key.equals(CleanType.ShiJianZhuanHuan.getCode())) {
                map.put("code", CleanType.ShiJianZhuanHuan.getCode());
                map.put("order", json.get(CleanType.ShiJianZhuanHuan.getCode()));
                returnList.add(map);
            } else if (key.equals(CleanType.MaZhiZhuanHuan.getCode())) {
                map.put("code", CleanType.MaZhiZhuanHuan.getCode());
                map.put("order", json.get(CleanType.MaZhiZhuanHuan.getCode()));
                returnList.add(map);
            } else if (key.equals(CleanType.ZiFuHeBing.getCode())) {
                map.put("code", CleanType.ZiFuHeBing.getCode());
                map.put("order", json.get(CleanType.ZiFuHeBing.getCode()));
                returnList.add(map);
            } else if (key.equals(CleanType.ZiFuChaiFen.getCode())) {
                map.put("code", CleanType.ZiFuChaiFen.getCode());
                map.put("order", json.get(CleanType.ZiFuChaiFen.getCode()));
                returnList.add(map);
            } else if (key.equals(CleanType.ZiFuTrim.getCode())) {
                map.put("code", CleanType.ZiFuTrim.getCode());
                map.put("order", json.get(CleanType.ZiFuTrim.getCode()));
                returnList.add(map);
            } else {
                throw new BusinessException("系统不支持的清洗类型，清洗编码为:" + key);
            }
        }
        return returnList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Object> getColumnIdByColSetId(long colSetId) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到数据库采集任务");
        }
        return Dbo.queryOneColumnList("select column_id from " + TableColumn.TableName + " tc " + " join " + TableInfo.TableName + " ti on tc.table_id = ti.table_id " + " where ti.database_id = ?", colSetId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Object> getColumnIdByTableId(long tableId) {
        return Dbo.queryOneColumnList("select column_id from " + TableColumn.TableName + " where table_id = ?", tableId);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnIds", desc = "", range = "")
    @Param(name = "cleanType", desc = "", range = "")
    private void delColCleanByColIdAndType(List<Object> columnIds, String cleanType) {
        StringBuilder strSB = new StringBuilder("delete from " + ColumnClean.TableName + " where column_id in ( ");
        for (int j = 0; j < columnIds.size(); j++) {
            strSB.append((long) columnIds.get(j));
            if (j != columnIds.size() - 1)
                strSB.append(",");
        }
        strSB.append(" ) and clean_type = ? ");
        Dbo.execute(strSB.toString(), cleanType);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableIds", desc = "", range = "")
    @Param(name = "cleanType", desc = "", range = "")
    private void delTbCleanByTbIdAndType(List<Object> tableIds, String cleanType) {
        StringBuilder strSB = new StringBuilder("delete from " + TableClean.TableName + " where table_id in ( ");
        for (int j = 0; j < tableIds.size(); j++) {
            strSB.append((long) tableIds.get(j));
            if (j != tableIds.size() - 1)
                strSB.append(",");
        }
        strSB.append(" ) and clean_type = ? ");
        Dbo.execute(strSB.toString(), cleanType);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columnIds", desc = "", range = "")
    @Param(name = "sort", desc = "", range = "")
    private void updateCleanOrderByColId(List<Object> columnIds, String sort) {
        StringBuilder strSB = new StringBuilder("update " + TableColumn.TableName + " set tc_or = ? where column_id in ( ");
        for (int j = 0; j < columnIds.size(); j++) {
            strSB.append((long) columnIds.get(j));
            if (j != columnIds.size() - 1)
                strSB.append(",");
        }
        strSB.append(" )");
        DboExecute.updatesOrThrow(columnIds.size(), "更新字段清洗优先级失败", strSB.toString(), sort);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableIds", desc = "", range = "")
    @Param(name = "sort", desc = "", range = "")
    private void updateCleanOrderByTbId(List<Object> tableIds, String sort) {
        StringBuilder strSB = new StringBuilder("update " + TableInfo.TableName + " set ti_or = ? where table_id in ( ");
        for (int j = 0; j < tableIds.size(); j++) {
            strSB.append((long) tableIds.get(j));
            if (j != tableIds.size() - 1)
                strSB.append(",");
        }
        strSB.append(" )");
        DboExecute.updatesOrThrow(tableIds.size(), "更新表清洗优先级失败", strSB.toString(), sort);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "colSetId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Object> getTableIdByColSetId(long colSetId) {
        long count = Dbo.queryNumber("select count(1) from " + DatabaseSet.TableName + " where database_id = ?", colSetId).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (count != 1) {
            throw new BusinessException("未找到数据库采集任务");
        }
        return Dbo.queryOneColumnList("select table_id from " + TableInfo.TableName + " where database_id = ?", colSetId);
    }
}
