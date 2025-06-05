package hyren.serv6.r.record.service.impl;

import fd.ng.core.utils.CodecUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.User;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.r.record.Req.SelectData;
import hyren.serv6.r.record.Res.DfTableApplyInfo;
import hyren.serv6.r.record.Res.ResData;
import hyren.serv6.r.record.service.DfTableApplyService;
import hyren.serv6.r.record.util.ExcelUtil;
import hyren.serv6.r.util.CreateTable;
import hyren.serv6.r.util.TempTableConf;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import static hyren.daos.bizpot.commons.Dbo.*;

@Service
public class DfTableApplyServiceImpl implements DfTableApplyService {

    private static final Logger log = LoggerFactory.getLogger(DfTableApplyServiceImpl.class);

    private String filePath = System.getProperty("user.dir");

    private static final String XLSX = ".xlsx";

    private static final String TABLE_PREFIX = "hyren_";

    @Override
    public void exportDslTable(Long dfAppId) {
        Map<String, Object> maps = queryOneObject("SELECT dsl_id FROM df_table_apply dta, df_pro_info dpi " + " WHERE dpi.df_pid = dta.df_pid AND apply_tab_id = " + dfAppId);
        long dslId = Long.parseLong(maps.get("dsl_id").toString());
        try (DatabaseWrapper dbWrapper = ConnectionTool.getDBWrapper(Dbo.db(), dslId)) {
            String tableName = "";
            List<Map<String, Object>> tableNameDb = Dbo.queryList("SELECT " + " dsl_table_name_id  " + " FROM " + " df_table_apply  " + " WHERE " + " apply_tab_id = " + dfAppId);
            for (Map<String, Object> stringObjectMap : tableNameDb) {
                if (Objects.isNull(stringObjectMap.get("dsl_table_name_id"))) {
                    throw new BusinessException("临时表不存在");
                }
                tableName = stringObjectMap.get("dsl_table_name_id").toString();
            }
            List<String> columns = getColumnData(dbWrapper, dfAppId);
            List<List<String>> tableData = getTableData(columns, dbWrapper, tableName);
            List<List<String>> resultData = new ArrayList<>(columns.size() + tableData.size());
            List<Map<String, Object>> columnList = Dbo.queryList("SELECT dtc.col_ch_name,dtc.col_name, dtc.is_primarykey FROM " + " df_table_apply dta  JOIN df_table_column dtc ON dtc.apply_tab_id = dta.apply_tab_id WHERE " + " dta.dsl_table_name_id = ?", tableName);
            List<String> newColumn = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                boolean flag = false;
                for (Map<String, Object> objectMap : columnList) {
                    if (columns.get(i).equals(objectMap.get("col_name").toString())) {
                        String col = objectMap.get("col_ch_name") == null ? StringUtil.EMPTY : objectMap.get("col_ch_name").toString();
                        if (Integer.parseInt(objectMap.get("is_primarykey").toString()) == 1) {
                            newColumn.add(col + "-" + objectMap.get("col_name").toString() + "-1");
                        } else {
                            newColumn.add(col + "-" + objectMap.get("col_name").toString());
                        }
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    newColumn.add(columns.get(i));
                }
            }
            resultData.add(newColumn);
            for (int i = 0; i < tableData.size(); i++) {
                List<String> objects = tableData.get(i);
                List<String> stringData = new ArrayList<>();
                for (int j = 0; j < objects.size(); j++) {
                    stringData.add(!Objects.isNull(objects.get(j)) ? objects.get(j) : "");
                }
                resultData.add(stringData);
            }
            recordExportExcel(arrayToObj(resultData));
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        }
    }

    @Override
    public void createTempTable(List<String> chKeyData, List<Map<String, String>> data, Long dfPid, Long table_id, String table_Name) {
        List<String> keyData = new ArrayList<>();
        for (String keyDatum : chKeyData) {
            if (keyDatum.contains("-1")) {
                String key = keyDatum.substring(keyDatum.indexOf("-") + 1, keyDatum.indexOf("-1"));
                keyData.add(key);
            } else if (keyDatum.contains("-")) {
                String key = keyDatum.substring(keyDatum.indexOf("-") + 1);
                keyData.add(key);
            } else {
                keyData.add(keyDatum);
            }
        }
        List<List<String>> dataArray = objToArray(data);
        List<String> dataColumn = dataArray.get(0);
        List<String> newdataColumn = new ArrayList<>();
        for (int i = 0; i < dataColumn.size(); i++) {
            if (dataColumn.get(i).contains("-")) {
                String chColumn = dataColumn.get(i).substring(0, dataColumn.get(i).indexOf("-"));
                String column = dataColumn.get(i).substring(chColumn.length() + 1);
                if (column.contains("-1")) {
                    String substring = column.substring(0, column.indexOf("-1"));
                    newdataColumn.add(substring);
                } else {
                    newdataColumn.add(column);
                }
            } else {
                newdataColumn.add(dataColumn.get(i));
            }
        }
        List<List<String>> tmp = new ArrayList<>();
        tmp.add(newdataColumn);
        for (int i = 1; i < dataArray.size(); i++) {
            tmp.add(dataArray.get(i));
        }
        data = arrayToObj(tmp);
        OptionalLong dsl_idOp = Dbo.queryNumber("select dsl_id from " + DfProInfo.TableName + " where df_pid = ?", dfPid);
        if (keyData.isEmpty()) {
            throw new BusinessException("请选择主键字段");
        }
        Long tailNum;
        String querySql1 = "SELECT COALESCE(count(1),0) as number FROM df_table_apply WHERE " + "table_id = " + table_id + "";
        Map<String, Object> CountMap = queryOneObject(querySql1);
        if (CountMap.get("number").toString().equals("0")) {
            tailNum = Long.parseLong(CountMap.get("number").toString());
        } else {
            String querySql2 = "SELECT dsl_table_name_id FROM df_table_apply WHERE" + " table_id = " + table_id + " " + "order by create_date desc,create_time desc ";
            List<Map<String, Object>> maps = queryList(querySql2);
            Map<String, Object> maxMap = maps.get(0);
            String tempName = maxMap.get("dsl_table_name_id").toString();
            tailNum = Long.parseLong(tempName.substring(tempName.lastIndexOf("_") + 1)) + 1;
        }
        String tableName = TABLE_PREFIX + table_Name.toLowerCase() + "_" + tailNum;
        DqTableInfo dqTableInfo = new DqTableInfo();
        dqTableInfo.setTable_id(PrimayKeyGener.getNextId());
        dqTableInfo.setTable_name(tableName);
        dqTableInfo.setCreate_date(DateUtil.getSysDate());
        dqTableInfo.setEnd_date(Constant.MAXDATE);
        User user = UserUtil.getUser();
        dqTableInfo.setCreate_id(user.getUserId());
        dqTableInfo.setIs_trace(IsFlag.Fou.getCode());
        dqTableInfo.setTable_space("");
        List<Map<String, Object>> query_list = Dbo.queryList("SELECT " + " tc.table_id, " + " tc.column_name, " + " tc.column_ch_name," + " tsm.column_tar_type as column_type " + " FROM " + " table_column tc " + " left JOIN tbcol_srctgt_map tsm ON tc.column_id = tsm.column_id " + " where " + " tc.table_id = ? ", table_id);
        List<List<String>> datas = objToArray(data);
        List<Map<String, String>> hyrenList = new ArrayList<Map<String, String>>();
        for (Map<String, String> map : data) {
            Set set = map.keySet();
            Iterator it = set.iterator();
            Map<String, String> stMap = new HashMap<String, String>();
            while (it.hasNext()) {
                String key = ((String) it.next()).toLowerCase();
                switch(key) {
                    case "hyren_s_date":
                    case "hyren_oper_time":
                    case "hyren_md5_val":
                    case "hyren_oper_person":
                    case "hyren_oper_date":
                    case "hyren_e_date":
                        stMap.put(key, map.get(key));
                        it.remove();
                        break;
                }
            }
            if (!stMap.isEmpty()) {
                hyrenList.add(stMap);
            }
        }
        List<DfTableColumn> dfTableColumns = new ArrayList<DfTableColumn>();
        if (!data.isEmpty()) {
            List<List<String>> lists = objToArray(data);
            Integer count = 0;
            for (Map<String, Object> map : query_list) {
                for (String tempColumn : lists.get(0)) {
                    if ((tempColumn).equals((map.get("column_name")))) {
                        String column_type = StringUtil.EMPTY;
                        if (map.get("column_type") == null) {
                            Map<String, Object> column_name = queryOneObject("select column_type from " + TableColumn.TableName + " where" + " table_id = ? and column_name = ?", table_id, map.get("column_name"));
                            column_type = column_name.get("column_type").toString();
                        } else {
                            column_type = map.get("column_type").toString();
                        }
                        DfTableColumn dfTableColumn = new DfTableColumn();
                        dfTableColumn.setApply_col_id(PrimayKeyGener.getNextId());
                        dfTableColumn.setCol_name(tempColumn);
                        dfTableColumn.setCol_ch_name((String) map.get("column_ch_name"));
                        dfTableColumn.setCol_type(column_type);
                        if (keyData.contains(tempColumn)) {
                            dfTableColumn.setIs_primarykey(IsFlag.Shi.getCode());
                        } else {
                            dfTableColumn.setIs_primarykey(IsFlag.Fou.getCode());
                        }
                        dfTableColumn.setApply_tab_id(dqTableInfo.getTable_id());
                        dfTableColumns.add(dfTableColumn);
                        count++;
                        break;
                    }
                }
            }
            if (count < lists.get(0).size()) {
                List<String> stList = new ArrayList<String>();
                Map<Object, Object> columnMap = new HashMap<Object, Object>();
                for (DfTableColumn dfTableColumn : dfTableColumns) {
                    columnMap.put(dfTableColumn.getCol_name(), 1);
                }
                for (String value : lists.get(0)) {
                    if (columnMap.get(value) == null) {
                        stList.add(value);
                    }
                }
                throw new BusinessException("字段名称和原表不匹配，不匹配字段为：" + stList);
            }
        }
        if (!hyrenList.isEmpty()) {
            final String HYREN_COLUMN_TYPE = "VARCHAR(100)";
            List<List<String>> hyrens = objToArray(hyrenList);
            for (String hyrenCol : hyrens.get(0)) {
                DfTableColumn Column = new DfTableColumn();
                Column.setApply_col_id(PrimayKeyGener.getNextId());
                Column.setCol_name(hyrenCol);
                Column.setCol_type(HYREN_COLUMN_TYPE);
                Column.setApply_tab_id(dqTableInfo.getTable_id());
                Column.setIs_primarykey(IsFlag.Fou.getCode());
                dfTableColumns.add(Column);
            }
        }
        DfTableColumn lastColumn = new DfTableColumn();
        lastColumn.setApply_col_id(PrimayKeyGener.getNextId());
        lastColumn.setCol_name(TempTableConf.OPERATION_COLUMN_NAME.toLowerCase());
        lastColumn.setCol_type(TempTableConf.OPERATION_COLUMN_TYPE.toLowerCase());
        lastColumn.setApply_tab_id(dqTableInfo.getTable_id());
        lastColumn.setIs_primarykey(IsFlag.Fou.getCode());
        dfTableColumns.add(lastColumn);
        List<List<String>> dataList = datas.subList(1, datas.size());
        Integer countData = 0;
        boolean flag = true;
        for (List<String> strList : dataList) {
            for (String str : strList) {
                if (str.equals("")) {
                    countData++;
                }
            }
        }
        if (countData == dataList.get(0).size()) {
            flag = false;
        }
        if (flag) {
            LayerBean layerBean = SqlOperator.queryOneObject(Dbo.db(), LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_idOp.getAsLong()).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL失败!")));
            DatabaseWrapper dbDataConn = null;
            try {
                dbDataConn = ConnectionTool.getDBWrapper(Dbo.db(), dsl_idOp.getAsLong());
                CreateTable.createDataTableByStorageLayer(dbDataConn, layerBean, dqTableInfo, dfTableColumns);
                DfTableApply dfTableApply = new DfTableApply();
                dfTableApply.setApply_tab_id(dqTableInfo.getTable_id());
                dfTableApply.setDf_pid(dfPid);
                dfTableApply.setTable_id(table_id);
                dfTableApply.setDep_id(user.getDepId());
                dfTableApply.setCreate_user_id(user.getUserId());
                dfTableApply.setCreate_date(DateUtil.getSysDate());
                dfTableApply.setCreate_time(DateUtil.getSysTime());
                dfTableApply.setUpdate_date(DateUtil.getSysDate());
                dfTableApply.setUpdate_time(DateUtil.getSysTime());
                dfTableApply.setDsl_table_name_id(dqTableInfo.getTable_name());
                dfTableApply.setIs_rec(IsFlag.Fou.getCode());
                dfTableApply.setIs_sync(IsFlag.Fou.getCode());
                dfTableApply.add(Dbo.db());
                for (String primaryKey : keyData) {
                    for (DfTableColumn dfTableColumn : dfTableColumns) {
                        if (primaryKey.toLowerCase().equals(dfTableColumn.getCol_name().toLowerCase())) {
                            dfTableColumn.setIs_primarykey(IsFlag.Shi.getCode());
                        }
                    }
                }
                dfTableColumns.forEach(dfTableColumn -> dfTableColumn.add(Dbo.db()));
                executeInsertSql(dqTableInfo.getTable_name(), dbDataConn, datas, table_id, dqTableInfo.getTable_id(), "");
                dfTableApply.setIs_rec(IsFlag.Shi.getCode());
                dfTableApply.update(Dbo.db());
            } catch (Exception e) {
                log.info("关系型数据库创建表时或保存临时表数据时发生异常,回滚此次存储层的db操作!");
                dbDataConn.execute(" drop table " + tableName);
                e.printStackTrace();
                throw new BusinessException("创建存储层数表或者保存临时表数据时发生异常!" + e.getMessage());
            } finally {
                if (null != dbDataConn) {
                    dbDataConn.commit();
                    dbDataConn.close();
                    log.info("关闭存储层db连接成功!");
                }
            }
            log.info("保存表源信息到配置库成功! table_name: " + dqTableInfo.getTable_name());
        } else {
            LayerBean layerBean = SqlOperator.queryOneObject(Dbo.db(), LayerBean.class, "select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_idOp.getAsLong()).orElseThrow(() -> (new BusinessException("获取存储层数据信息的SQL失败!")));
            DatabaseWrapper dbDataConn = null;
            try {
                dbDataConn = ConnectionTool.getDBWrapper(Dbo.db(), dsl_idOp.getAsLong());
                CreateTable.createDataTableByStorageLayer(dbDataConn, layerBean, dqTableInfo, dfTableColumns);
                DfTableApply dfTableApply = new DfTableApply();
                dfTableApply.setApply_tab_id(dqTableInfo.getTable_id());
                dfTableApply.setDf_pid(dfPid);
                dfTableApply.setTable_id(table_id);
                dfTableApply.setDep_id(user.getDepId());
                dfTableApply.setCreate_user_id(user.getUserId());
                dfTableApply.setCreate_date(DateUtil.getSysDate());
                dfTableApply.setCreate_time(DateUtil.getSysTime());
                dfTableApply.setUpdate_date(DateUtil.getSysDate());
                dfTableApply.setUpdate_time(DateUtil.getSysTime());
                dfTableApply.setDsl_table_name_id(dqTableInfo.getTable_name());
                dfTableApply.setIs_rec(IsFlag.Fou.getCode());
                dfTableApply.setIs_sync(IsFlag.Fou.getCode());
                dfTableApply.add(Dbo.db());
                for (String primaryKey : keyData) {
                    for (DfTableColumn dfTableColumn : dfTableColumns) {
                        if (primaryKey.toLowerCase().equals(dfTableColumn.getCol_name().toLowerCase())) {
                            dfTableColumn.setIs_primarykey(IsFlag.Shi.getCode());
                        }
                    }
                }
                dfTableColumns.forEach(dfTableColumn -> dfTableColumn.add(Dbo.db()));
            } catch (Exception e) {
                dbDataConn.rollback();
                log.info("关系型数据库创建表时或保存临时表数据时发生异常,回滚此次存储层的db操作!");
                e.printStackTrace();
                throw new BusinessException("创建存储层数表或者保存临时表数据时发生异常!" + e.getMessage());
            } finally {
                if (null != dbDataConn) {
                    dbDataConn.commit();
                    dbDataConn.close();
                    log.info("关闭存储层db连接成功!");
                }
            }
        }
    }

    private void stringSql(int v, List<String> strings, List<String> column, Assembler dataSql) {
        if (v == column.size() - 1 && !StringUtil.isBlank(strings.get(column.size() - 1))) {
            dataSql.addSql("'" + strings.get(column.size() - 1) + "')");
        } else if (v == column.size() - 1 && StringUtil.isBlank(strings.get(column.size() - 1))) {
            dataSql.addSql("null)");
        } else if (StringUtil.isBlank(strings.get(v))) {
            dataSql.addSql("null,");
        } else {
            dataSql.addSql("'" + strings.get(v) + "',");
        }
    }

    @Override
    public ResData findList(SelectData dfId, long targetTableId) {
        Map<String, Object> map = Dbo.queryOneObject("select dsl_id from df_pro_info where df_pid = " + dfId.getDfPid());
        long dfPid = Long.parseLong(map.get("dsl_id").toString());
        try (DatabaseWrapper wrapper = ConnectionTool.getDBWrapper(Dbo.db(), dfPid)) {
            List<List<String>> result = new ArrayList<>();
            List<Map<String, Object>> columnList = Dbo.queryList("SELECT tc.is_primary_key,tc.column_name,tc.column_ch_name FROM table_column tc " + " left JOIN tbcol_srctgt_map tsm ON tc.column_id = tsm.column_id " + " WHERE tc.table_id = ?", targetTableId);
            List<String> column = new ArrayList<>();
            List<String> engColumn = new ArrayList<>();
            for (int i = 0; i < columnList.size(); i++) {
                if (Integer.parseInt(columnList.get(i).get("is_primary_key").toString()) == 1) {
                    column.add(columnList.get(i).get("column_ch_name") + "-" + columnList.get(i).get("column_name").toString() + "-1");
                } else {
                    column.add(columnList.get(i).get("column_ch_name") + "-" + columnList.get(i).get("column_name").toString());
                }
                engColumn.add(columnList.get(i).get("column_name").toString());
            }
            result.add(column);
            List<List<String>> tableData = getTableData(engColumn, wrapper, dfId.getTableName());
            List<List<String>> resultData = new ArrayList<>();
            int count = tableData.size();
            int start = (dfId.getCurrPage() - 1) * dfId.getPageSize();
            int down = start + dfId.getPageSize();
            if (down > count) {
                resultData = tableData.subList(start, count);
            } else {
                resultData = tableData.subList(start, down);
            }
            result.addAll(resultData);
            List<Map<String, String>> resultMap = arrayToObj(result);
            ResData resData = getResData(resultMap, tableData.size());
            List<Map<String, Object>> maps = Dbo.queryList("SELECT column_name FROM table_column WHERE is_primary_key = '1' " + "AND table_id  = " + targetTableId);
            List<String> keyData = new ArrayList<>();
            for (Map<String, Object> objectMap : maps) {
                keyData.add(objectMap.get("column_name").toString());
            }
            resData.setKeyData(keyData);
            return resData;
        } catch (BusinessException e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
    }

    @Override
    public void exportCursor(SelectData dfId) {
        Map<String, Object> map = Dbo.queryOneObject("select dsl_id from df_pro_info where df_pid = " + dfId.getDfPid());
        long dfPid = Long.parseLong(map.get("dsl_id").toString());
        try (DatabaseWrapper wrapper = ConnectionTool.getDBWrapper(Dbo.db(), dfPid)) {
            List<List<String>> result = new ArrayList<>();
            List<Map<String, Object>> columnList = Dbo.queryList("SELECT tc.is_primary_key,tc.column_name,tc.column_ch_name FROM table_column tc " + " left JOIN tbcol_srctgt_map tsm ON tc.column_id = tsm.column_id " + " WHERE tc.table_id = ?", dfId.getTargetTableId());
            List<String> column = new ArrayList<>();
            List<String> engColumn = new ArrayList<>();
            for (int i = 0; i < columnList.size(); i++) {
                if (Integer.parseInt(columnList.get(i).get("is_primary_key").toString()) == 1) {
                    column.add(columnList.get(i).get("column_ch_name") + "-" + columnList.get(i).get("column_name").toString() + "-1");
                } else {
                    column.add(columnList.get(i).get("column_ch_name") + "-" + columnList.get(i).get("column_name").toString());
                }
                engColumn.add(columnList.get(i).get("column_name").toString());
            }
            result.add(column);
            List<List<String>> tableData = getTableData(engColumn, wrapper, dfId.getTableName());
            result.addAll(tableData);
            List<Map<String, String>> resultMap = arrayToObj(result);
            recordExportExcel(resultMap);
        } catch (BusinessException e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
    }

    @Override
    public void recordExportExcel(List<Map<String, String>> lists) {
        List<List<String>> data = objToArray(lists);
        File result = null;
        try {
            long flagId = System.currentTimeMillis();
            result = new File(filePath + File.separator + System.currentTimeMillis() + "_RECORD" + XLSX);
            OutputStream outputStream = new FileOutputStream(result);
            Workbook dataToWorkbook = ExcelUtil.createDataToWorkbook(data, new XSSFWorkbook());
            ExcelUtil.writeWorkbookToOutputStream(dataToWorkbook, outputStream);
            downloadDistributeFile(flagId + "_RECORD" + XLSX);
        } catch (IOException e) {
            e.printStackTrace();
            result.delete();
            log.error("recordExportExcel failed.");
        } finally {
            if (result != null) {
                result.delete();
            }
        }
    }

    @Override
    public ResData importExcel(MultipartFile excel) {
        File result = null;
        try {
            String originalFilename = excel.getOriginalFilename();
            result = new File(filePath + File.pathSeparator + originalFilename);
            excel.transferTo(result);
            List<List<Object>> lists = ExcelUtil.readExcel(result);
            List<List<String>> list = new ArrayList<>();
            for (int i = 0; i < lists.size(); i++) {
                List<Object> objects = lists.get(i);
                List<String> stringList = new ArrayList<>();
                for (int j = 0; j < objects.size(); j++) {
                    if (Objects.isNull(objects.get(j))) {
                        stringList.add(StringUtil.EMPTY);
                    } else {
                        stringList.add(objects.get(j).toString());
                    }
                }
                list.add(stringList);
            }
            if (!Objects.isNull(lists.get(0))) {
                List<String> objects = list.get(0);
                if (StringUtil.isBlank(objects.get(objects.size() - 1).toString())) {
                    objects.remove(objects.size() - 1);
                }
            }
            if (lists.size() > 2001) {
                throw new BusinessException("上传excel数据超过2000条，请取消后重新上传.");
            }
            List<Map<String, String>> maps = null;
            List<String> keyData = new ArrayList<>();
            try {
                List<String> columns = list.get(0);
                if (columns.isEmpty()) {
                    throw new BusinessException("数据有误，请检查");
                }
                for (String column : columns) {
                    if (column.endsWith("-1")) {
                        keyData.add(column);
                    }
                }
                maps = arrayToObj(list);
            } catch (Exception e) {
                throw new BusinessException("上传excel数据格式请按照表格形式");
            }
            ResData resData = new ResData();
            resData.setKeyData(keyData);
            resData.setData(maps);
            return resData;
        } catch (IOException e) {
            e.printStackTrace();
            throw new BusinessException("上传文件失败.");
        } finally {
            if (result != null) {
                result.delete();
            }
        }
    }

    private void downloadDistributeFile(String fileName) {
        FileInputStream in = null;
        try (OutputStream out = ContextDataHolder.getResponse().getOutputStream()) {
            String filePaths = filePath + File.separator + fileName;
            ContextDataHolder.getResponse().reset();
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.ISO_8859_1.getValue()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            in = new FileInputStream(filePaths);
            byte[] bytes = new byte[1024];
            int len;
            while ((len = in.read(bytes)) > 0) {
                out.write(bytes, 0, len);
            }
            out.flush();
        } catch (UnsupportedEncodingException e) {
            throw new BusinessException("不支持的编码异常");
        } catch (FileNotFoundException e) {
            throw new BusinessException("文件不存在，可能目录不存在！");
        } catch (IOException e) {
            throw new BusinessException("下载文件失败！");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error(e.toString());
                }
            }
        }
    }

    @Override
    public boolean updateStatus(Long dfId) {
        Map<String, Object> map = Dbo.queryOneObject("SELECT dsl_id FROM df_table_apply dta,df_pro_info dpi WHERE " + " dpi.df_pid = dta.df_pid AND apply_tab_id = " + dfId);
        if (Objects.isNull(map.get("dsl_id"))) {
            throw new BusinessException("存储层连接参数有误，请检查。");
        }
        long dslId = Long.parseLong(map.get("dsl_id").toString());
        try (DatabaseWrapper dbWrapper = ConnectionTool.getDBWrapper(Dbo.db(), dslId)) {
            String tableName = "";
            List<Map<String, Object>> tableNameDb = Dbo.queryList("SELECT dsl_table_name_id FROM df_table_apply  " + " WHERE apply_tab_id = " + dfId);
            for (Map<String, Object> stringObjectMap : tableNameDb) {
                if (Objects.isNull(stringObjectMap.get("dsl_table_name_id"))) {
                    throw new BusinessException("临时表不存在!");
                }
                tableName = stringObjectMap.get("dsl_table_name_id").toString();
            }
            try {
                Dbo.execute(dbWrapper, " drop table " + tableName);
                dbWrapper.commit();
            } catch (Exception e) {
                e.printStackTrace();
                dbWrapper.rollback();
                throw new BusinessException("临时表不存在!");
            }
            try {
                Dbo.execute("delete from " + DfTableApply.TableName + " where apply_tab_id = " + dfId);
                Dbo.execute("delete from df_table_column where apply_tab_id = " + dfId);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                throw new BusinessException("临时表删除失败!");
            }
        } catch (Exception e) {
            throw new BusinessException("获取存储层连接失败。");
        }
    }

    private DatabaseWrapper getWrapper(Long dfId) {
        Map<String, Object> map = Dbo.queryOneObject(" SELECT dpi.dsl_id FROM df_table_apply dta,df_pro_info dpi " + " WHERE dta.df_pid = dpi.df_pid AND dta.apply_tab_id = " + dfId);
        long dbId = Long.parseLong(map.get("dsl_id").toString());
        DatabaseWrapper dbWrapper = null;
        try {
            dbWrapper = ConnectionTool.getDBWrapper(Dbo.db(), dbId);
            return dbWrapper;
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("存储层连接超时，请刷新页面重新操作.");
        } finally {
            dbWrapper.close();
        }
    }

    @Override
    public ResData findCursorList(SelectData dfId) {
        Map<String, Object> maps = queryOneObject("select dsl_table_name_id from " + DfTableApply.TableName + " where apply_tab_id = " + dfId.getDfPid());
        Object dsl_table_name_id = maps.get("dsl_table_name_id");
        Map<String, Object> longs = queryOneObject(" SELECT dpi.dsl_id FROM df_table_apply dta,df_pro_info dpi " + " WHERE dta.df_pid = dpi.df_pid AND dta.apply_tab_id = " + dfId.getDfPid());
        Object dsl_id = longs.get("dsl_id");
        if (Objects.isNull(dsl_id) || Objects.isNull(dsl_table_name_id)) {
            throw new BusinessException("临时表与连接信息错误,请检查。");
        }
        long dbId = Long.parseLong(dsl_id.toString());
        String tableName = dsl_table_name_id.toString();
        try (DatabaseWrapper dbWrapper = ConnectionTool.getDBWrapper(Dbo.db(), dbId)) {
            List<String> columns = getColumnData(dbWrapper, dfId.getDfPid());
            List<List<String>> tableData = getTableData(columns, dbWrapper, tableName);
            List<List<String>> resultData = new ArrayList<>(columns.size() + tableData.size());
            List<List<String>> result = new ArrayList<>();
            if (dfId.getCurrPage() != 0 && dfId.getPageSize() != 0) {
                int count = tableData.size();
                int start = (dfId.getCurrPage() - 1) * dfId.getPageSize();
                int down = start + dfId.getPageSize();
                if (down > count) {
                    result = tableData.subList(start, count);
                } else {
                    result = tableData.subList(start, down);
                }
            } else {
                result = tableData;
            }
            resultData.add(getChColumn(tableName, columns));
            resultData.addAll(result);
            List<Map<String, String>> resultMap = arrayToObj(resultData);
            return getResData(resultMap, tableData.size());
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
    }

    public List<Map<String, String>> arrayToObj(List<List<String>> data) {
        List<Map<String, String>> result = new ArrayList<>();
        if (data.isEmpty()) {
            throw new BusinessException("数据有误，请检查");
        }
        List<String> column = data.get(0);
        if (column.isEmpty()) {
            throw new BusinessException("数据有误，请检查");
        }
        List<List<String>> rows = data.subList(1, data.size());
        if (rows.size() != 0) {
            for (int j = 0; j < rows.size(); j++) {
                List<String> strings = rows.get(j);
                Map<String, String> map = new HashMap<>();
                for (int i = 0; i < strings.size(); i++) {
                    map.put(column.get(i), strings.get(i));
                }
                result.add(map);
            }
        } else {
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < column.size(); i++) {
                map.put(column.get(i), StringUtil.EMPTY);
            }
            result.add(map);
        }
        return result;
    }

    public List<List<String>> objToArray(List<Map<String, String>> data) {
        List<List<String>> result = new ArrayList<>();
        if (data.isEmpty()) {
            throw new BusinessException("数据有误，请检查");
        }
        List<String> column = new ArrayList<String>(data.get(0).keySet());
        result.add(column);
        for (int i = 0; i < data.size(); i++) {
            List<Object> rowObj = new ArrayList<>(data.get(i).values());
            List<String> dataRow = new ArrayList<>();
            for (int j = 0; j < rowObj.size(); j++) {
                if (Objects.isNull(rowObj.get(j))) {
                    dataRow.add("");
                } else {
                    dataRow.add(rowObj.get(j).toString());
                }
            }
            result.add(dataRow);
        }
        return result;
    }

    private List<String> getChColumn(String tableName, List<String> columns) {
        List<Map<String, Object>> columnList = Dbo.queryList("SELECT dtc.col_ch_name,dtc.col_name, dtc.is_primarykey FROM " + " df_table_apply dta  JOIN df_table_column dtc ON dtc.apply_tab_id = dta.apply_tab_id WHERE " + " dta.dsl_table_name_id = ?", tableName);
        List<String> newColumn = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            boolean flag = false;
            for (Map<String, Object> objectMap : columnList) {
                if (columns.get(i).equals(objectMap.get("col_name").toString())) {
                    String col = objectMap.get("col_ch_name") == null ? StringUtil.EMPTY : objectMap.get("col_ch_name").toString();
                    if (Integer.parseInt(objectMap.get("is_primarykey").toString()) == 1) {
                        newColumn.add(col + "-" + objectMap.get("col_name").toString() + "-1");
                    } else {
                        newColumn.add(col + "-" + objectMap.get("col_name").toString());
                    }
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                newColumn.add(columns.get(i));
            }
        }
        return newColumn;
    }

    private List<String> getColumnData(DatabaseWrapper dbWrapper, Long targetTableId) {
        String tableSql = "select col_name from df_table_column where apply_tab_id =" + targetTableId;
        List<String> columns = new ArrayList<>();
        List<Map<String, Object>> maps1 = Dbo.queryList(tableSql);
        for (int i = 0; i < maps1.size(); i++) {
            if (maps1.get(i).get("col_name").toString().equals(TempTableConf.OPERATION_COLUMN_NAME.toLowerCase())) {
                continue;
            }
            columns.add(maps1.get(i).get("col_name").toString());
        }
        return columns;
    }

    private List<List<String>> getTableData(List<String> columns, DatabaseWrapper dbWrapper, String tableName) {
        List<Map<String, Object>> data = null;
        Assembler assembler = Assembler.newInstance();
        for (int i = 0; i < columns.size(); i++) {
            if (i == columns.size() - 1) {
                assembler.addSql(columns.get(i));
            } else {
                assembler.addSql(columns.get(i) + ",");
            }
        }
        try {
            data = SqlOperator.queryList(dbWrapper, " select " + assembler.sql() + " from " + tableName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("存储表不存在。");
        }
        List<List<String>> tableData = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> stringObjectMap = data.get(i);
            List<String> tableRow = new ArrayList<>();
            for (int j = 0; j < columns.size(); j++) {
                Object column = stringObjectMap.get(columns.get(j).toLowerCase());
                if (TempTableConf.OPERATION_COLUMN_NAME.toLowerCase().equals(columns.get(j))) {
                    continue;
                }
                if (Objects.isNull(column)) {
                    tableRow.add(StringUtil.EMPTY);
                } else {
                    tableRow.add(column.toString());
                }
            }
            tableData.add(tableRow);
        }
        return tableData;
    }

    @Override
    public ResData findAllListByName(SelectData page) {
        String sql = " SELECT DISTINCT tsi.hyren_name AS TABLE_NAME,ti.table_ch_name,ti.table_id as target_table_id, " + " dta.* FROM data_store_layer dsl JOIN dtab_relation_store drs ON dsl.dsl_id = drs.dsl_id " + " JOIN table_storage_info tsi ON drs.tab_id = tsi.storage_id " + " JOIN table_info ti ON tsi.table_id = ti.table_id " + " JOIN df_pro_info dpi ON dpi.dsl_id = drs.dsl_id " + " left join df_table_apply dta on dta.table_id = ti.table_id and dta.df_pid = dpi.df_pid " + " WHERE " + " dpi.df_pid = " + page.getDfPid();
        if (!StringUtil.isBlank(page.getTableName())) {
            sql = " SELECT DISTINCT tsi.hyren_name AS TABLE_NAME,ti.table_ch_name, ti.table_id as target_table_id, " + " dta.* FROM  data_store_layer dsl  JOIN dtab_relation_store drs ON dsl.dsl_id = drs.dsl_id " + " JOIN table_storage_info tsi ON drs.tab_id = tsi.storage_id " + " JOIN table_info ti ON tsi.table_id = ti.table_id " + " JOIN df_pro_info dpi ON dpi.dsl_id = drs.dsl_id " + " left join df_table_apply dta on dta.table_id = ti.table_id and dta.df_pid = dpi.df_pid " + " WHERE " + " dpi.df_pid = " + page.getDfPid() + " and tsi.hyren_name like '%" + page.getTableName() + "%'";
        }
        List<DfTableApplyInfo> dfTableApplyInfos = Dbo.queryPagedList(DfTableApplyInfo.class, page, sql);
        return getResData(dfTableApplyInfos, page.getTotalSize());
    }

    @Override
    public ResData findRecordListByName(SelectData page) {
        Assembler sql = Assembler.newInstance();
        sql.addSql(" SELECT di.dep_name,su.user_name,tsi.hyren_name as table_name, ti.table_ch_name, dta.* " + " FROM " + DfTableApply.TableName + " dta," + TableInfo.TableName + " ti, " + DepartmentInfo.TableName + " di," + SysUser.TableName + " su, table_storage_info tsi where dta.df_pid = " + page.getDfPid() + " and dta.is_rec= '1' and dta.dep_id = di.dep_id and dta.create_user_id = su.user_id" + " and dta.table_id = ti.table_id and tsi.table_id = ti.table_id");
        if (!StringUtil.isBlank(page.getTableName())) {
            sql.addSql(" and tsi.hyren_name like '%" + page.getTableName() + "%'");
        }
        if (!StringUtil.isBlank(page.getCreateUserId())) {
            sql.addSql(" and su.user_name like '%" + page.getCreateUserId() + "%'");
        }
        if (!StringUtil.isBlank(page.getCreateDate())) {
            sql.addSql(" and dta.create_date = '" + page.getCreateDate() + "'");
        }
        if (!StringUtil.isBlank(page.getUpdateDate())) {
            sql.addSql(" and dta.update_date = '" + page.getUpdateDate() + "'");
        }
        List<DfTableApplyInfo> dfTableApplyInfos = Dbo.queryPagedList(DfTableApplyInfo.class, page, sql.sql());
        return getResData(dfTableApplyInfos, page.getTotalSize());
    }

    @Override
    public ResData findAll(SelectData page) {
        List<DfTableApplyInfo> dfTableApplyInfos = Dbo.queryPagedList(DfTableApplyInfo.class, page, "  SELECT dsr.original_name AS TABLE_NAME, ti.table_ch_name, ti.table_id AS target_table_id, dta.*  FROM  data_store_layer dsl\n" + " JOIN dtab_relation_store drs ON dsl.dsl_id = drs.dsl_id\n" + " JOIN table_storage_info tsi ON drs.tab_id = tsi.storage_id\n" + " JOIN table_info ti ON tsi.table_id = ti.table_id\n" + " JOIN df_pro_info dpi ON dpi.dsl_id = drs.dsl_id\n" + " JOIN data_store_reg dsr ON dsr.table_id = tsi.table_id\n" + " JOIN database_set dbs ON dsr.database_id = dbs.database_id\n" + " LEFT JOIN df_table_apply dta ON dta.table_id = ti.table_id and dta.df_pid = dpi.df_pid \n" + " WHERE  dpi.df_pid = " + page.getDfPid() + "  and  dbs.collect_type = '1'  AND dsr.hyren_name = tsi.hyren_name  AND dsr.collect_type IN ( '4', '1' ) UNION\n" + " SELECT  tsi.hyren_name AS TABLE_NAME,  ti.table_ch_name, ti.table_id AS target_table_id, dta.*  FROM  data_store_layer dsl\n" + " JOIN dtab_relation_store drs ON dsl.dsl_id = drs.dsl_id\n" + " JOIN table_storage_info tsi ON drs.tab_id = tsi.storage_id\n" + " JOIN table_info ti ON tsi.table_id = ti.table_id\n" + " JOIN df_pro_info dpi ON dpi.dsl_id = drs.dsl_id\n" + " JOIN data_store_reg dsr ON dsr.table_id = tsi.table_id\n" + " JOIN database_set dbs ON dsr.database_id = dbs.database_id\n" + " LEFT JOIN df_table_apply dta ON dta.table_id = ti.table_id and dta.df_pid = dpi.df_pid \n" + " WHERE  dpi.df_pid = " + page.getDfPid() + "  and  dbs.collect_type != '1'  AND dsr.hyren_name = tsi.hyren_name  AND dsr.collect_type IN ( '4', '1' )");
        return getResData(dfTableApplyInfos, page.getTotalSize());
    }

    @Override
    public ResData findByRecordList(SelectData page) {
        List<DfTableApplyInfo> dfTableApplyInfos = Dbo.queryPagedList(DfTableApplyInfo.class, page, " SELECT di.dep_name,su.user_name,tsi.hyren_name AS TABLE_NAME,ti.table_ch_name,dta.* FROM df_table_apply dta\n" + "JOIN table_info ti ON dta.table_id = ti.table_id\n" + "JOIN department_info di ON dta.dep_id = di.dep_id\n" + "JOIN sys_user su ON dta.create_user_id = su.user_id\n" + "JOIN table_storage_info tsi ON tsi.table_id = ti.table_id\n" + "JOIN data_store_reg dsr ON dsr.table_id = tsi.table_id\n" + "JOIN database_set dbs ON dsr.database_id = dbs.database_id \n" + "WHERE dta.df_pid = ? AND dta.is_rec = '1' AND dbs.collect_type != '1' \n" + "AND dsr.hyren_name = tsi.hyren_name AND dsr.collect_type IN ( '4', '1' ) UNION\n" + "SELECT di.dep_name,su.user_name,dsr.original_name AS TABLE_NAME,ti.table_ch_name,dta.* FROM df_table_apply dta\n" + "JOIN table_info ti ON dta.table_id = ti.table_id\n" + "JOIN department_info di ON dta.dep_id = di.dep_id\n" + "JOIN sys_user su ON dta.create_user_id = su.user_id\n" + "JOIN table_storage_info tsi ON tsi.table_id = ti.table_id\n" + "JOIN data_store_reg dsr ON dsr.table_id = tsi.table_id\n" + "JOIN database_set dbs ON dsr.database_id = dbs.database_id \n" + "WHERE dta.df_pid = ? AND dta.is_rec = '1' AND dbs.collect_type = '1' \n" + "AND dsr.hyren_name = tsi.hyren_name AND dsr.collect_type IN ( '4', '1' )", page.getDfPid(), page.getDfPid());
        return getResData(dfTableApplyInfos, page.getTotalSize());
    }

    private ResData getResData(List data, int count) {
        return new ResData(count, data);
    }

    @Override
    public boolean updateCursor(Long applyId, List<Map<String, String>> list, Long targetTableId, Long dfId) {
        List<List<String>> data = objToArray(list);
        if (Objects.isNull(applyId)) {
            throw new BusinessException("数据有误，请检查。");
        }
        if (data.size() > 2000) {
            throw new BusinessException("临时表数据不能超过2000行。");
        }
        Map<String, Object> stringObjectMap = queryOneObject("SELECT  dsl_table_name_id  FROM  df_table_apply  " + " WHERE  apply_tab_id = " + applyId);
        if (Objects.isNull(stringObjectMap.get("dsl_table_name_id"))) {
            throw new BusinessException("临时表信息不存在");
        }
        String tableName = stringObjectMap.get("dsl_table_name_id").toString();
        Map<String, Object> dfPidMap = Dbo.queryOneObject(" SELECT dpi.dsl_id FROM df_pro_info dpi " + " WHERE dpi.df_pid = " + dfId);
        long dbId = Long.parseLong(dfPidMap.get("dsl_id").toString());
        try (DatabaseWrapper wrapper = ConnectionTool.getDBWrapper(Dbo.db(), dbId)) {
            List<String> column = data.get(0);
            column.remove("index");
            List<String> newdataColumn = new ArrayList<>();
            for (int i = 0; i < column.size(); i++) {
                if (column.get(i).contains("-")) {
                    String chColumn = column.get(i).substring(0, column.get(i).indexOf("-"));
                    String engColumn = column.get(i).substring(chColumn.length() + 1);
                    if (engColumn.contains("-1")) {
                        String substring = engColumn.substring(0, engColumn.indexOf("-"));
                        newdataColumn.add(substring);
                    } else {
                        newdataColumn.add(engColumn);
                    }
                } else {
                    newdataColumn.add(column.get(i));
                }
            }
            List<List<String>> newData = new ArrayList<>();
            newData.add(newdataColumn);
            for (int i = 1; i < data.size(); i++) {
                newData.add(data.get(i));
            }
            List<Map<String, Object>> maps = null;
            List<String> keys = new ArrayList<>();
            List<String> dbColumn = new ArrayList<>();
            try {
                maps = Dbo.queryList("SELECT col_name  " + " FROM " + " df_table_column  " + " WHERE " + " apply_tab_id = " + applyId);
                List<Map<String, Object>> keyMap = Dbo.queryList("SELECT COLUMN_NAME \n" + "FROM " + " table_column  " + " WHERE " + " table_id = " + targetTableId + " AND is_primary_key = '1'");
                if (!Objects.isNull(keyMap) && keyMap.size() != 0) {
                    for (Map<String, Object> objectMap : keyMap) {
                        keys.add(objectMap.get("column_name").toString());
                    }
                }
                for (Map<String, Object> map : maps) {
                    String columnName = map.get("col_name").toString();
                    dbColumn.add(columnName);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new BusinessException("临时表不存在。");
            }
            for (String s : newdataColumn) {
                if (s.equals(TempTableConf.OPERATION_COLUMN_NAME.toLowerCase())) {
                    continue;
                }
                if (!dbColumn.contains(s)) {
                    throw new BusinessException("数据补录字段和目标表字段不一致，请重新编辑。");
                }
            }
            try {
                Dbo.execute(wrapper, "DELETE FROM " + tableName + " WHERE 1 = 1 ");
                executeInsertSql(tableName, wrapper, newData, targetTableId, applyId, "update");
                updateDfApply(applyId);
                wrapper.commit();
                return true;
            } catch (BusinessException e) {
                e.printStackTrace();
                wrapper.rollback();
                throw new BusinessException(e.getMessage());
            }
        } catch (BusinessException e) {
            e.printStackTrace();
            throw new BusinessException(e.getMessage());
        }
    }

    private void updateDfApply(long applyId) {
        Dbo.execute("update " + DfTableApply.TableName + " set update_date = '" + DateUtil.getSysDate() + "', update_time = '" + DateUtil.getSysTime() + "' where apply_tab_id = " + applyId);
    }

    public void executeInsertSql(String tableName, DatabaseWrapper wrapper, List<List<String>> data, Long targetTableId, Long applyId, String flag) {
        List<String> column = data.get(0);
        List<List<String>> rowData = data.subList(1, data.size());
        String COLUMN_NAME = "col_name";
        String COLUMN_TYPE = "col_type";
        List<Map<String, Object>> allColumnTypeMap = Dbo.queryList("SELECT col_name,col_type  " + "FROM  df_table_column  WHERE  apply_tab_id = " + applyId);
        Assembler sql = Assembler.newInstance();
        Assembler dataSql = Assembler.newInstance();
        for (int i = 0; i < rowData.size(); i++) {
            sql.cleanParams();
            sql.clean();
            dataSql.clean();
            dataSql.cleanParams();
            sql.addSql("insert into " + tableName + "(");
            dataSql.addSql(" values (");
            String columnType = "";
            int columnTypeLength = 0;
            for (int v = 0; v <= column.size() - 1; v++) {
                List<String> strings = rowData.get(i);
                if (v == column.size() - 1) {
                    sql.addSql(column.get(column.size() - 1)).addSql(")");
                } else {
                    sql.addSql(column.get(v)).addSql(",");
                }
                for (Map<String, Object> objectMap : allColumnTypeMap) {
                    if (objectMap.get(COLUMN_NAME).toString().equals(column.get(v))) {
                        columnType = objectMap.get(COLUMN_TYPE).toString();
                        if (columnType.contains("(")) {
                            String substring = columnType.substring(columnType.indexOf("(") + 1, columnType.indexOf(")"));
                            if (substring.contains(",")) {
                                columnTypeLength = Integer.parseInt(substring.substring(0, substring.indexOf(",")));
                            } else {
                                columnTypeLength = Integer.parseInt(substring);
                            }
                        } else {
                            columnTypeLength = 255;
                        }
                        if (!StringUtil.isBlank(strings.get(v)) && strings.get(v).length() > columnTypeLength) {
                            throw new BusinessException("字段" + column.get(v) + "超过限定长度" + columnTypeLength + " 字符。");
                        }
                    }
                }
                if (columnType.contains("(")) {
                    columnType = columnType.substring(0, columnType.indexOf("("));
                }
                switch(columnType.toLowerCase()) {
                    case "int8":
                    case "int":
                    case "int2":
                    case "int4":
                    case "bigint":
                    case "integer":
                    case "number":
                    case "numeric":
                    case "decimal":
                        if (v == column.size() - 1 && !StringUtil.isBlank(strings.get(column.size() - 1))) {
                            dataSql.addSql(strings.get(column.size() - 1)).addSql(")");
                        } else if (v == column.size() - 1 && StringUtil.isBlank(strings.get(column.size() - 1))) {
                            dataSql.addSql("null)");
                        } else if (StringUtil.isBlank(strings.get(v))) {
                            dataSql.addSql("null,");
                        } else {
                            dataSql.addSql(strings.get(v)).addSql(",");
                        }
                        break;
                    case "char":
                    case "varchar":
                    case "text":
                        stringSql(v, strings, column, dataSql);
                        break;
                    case "time":
                        Pattern compile = Pattern.compile("([0-1][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])");
                        boolean matches = compile.matcher(strings.get(v)).matches();
                        if (!matches) {
                            throw new BusinessException("字段" + column.get(v) + "不符合规则 MM:HH:SS");
                        }
                        stringSql(v, strings, column, dataSql);
                        break;
                    case "date":
                        Pattern compileDate = Pattern.compile("^\\d{4}-((0\\d)|(1[012]))-(([012]\\d)|3[01])$");
                        boolean matchesDate = compileDate.matcher(strings.get(v)).matches();
                        if (!matchesDate) {
                            throw new BusinessException("字段" + column.get(v) + "不符合规则 yyyy-MM-dd");
                        }
                        stringSql(v, strings, column, dataSql);
                        break;
                    default:
                        stringSql(v, strings, column, dataSql);
                }
            }
            String targetSql = sql.sql() + dataSql.sql();
            try {
                Dbo.execute(wrapper, targetSql);
            } catch (Exception e) {
                throw new BusinessException("当前数据和目标表或临时表字段或字段类型不一致，请检查");
            }
        }
    }

    public static void main(String[] args) {
        String url = "1999-03-13";
        Pattern compileDate = Pattern.compile("^\\d{4}-((0\\d)|(1[012]))-(([012]\\d)|3[01])$");
        boolean matchesDate = compileDate.matcher(url).matches();
        System.out.println(matchesDate);
    }
}
