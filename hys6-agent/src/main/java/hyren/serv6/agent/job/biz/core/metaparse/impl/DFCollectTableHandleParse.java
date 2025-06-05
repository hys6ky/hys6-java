package hyren.serv6.agent.job.biz.core.metaparse.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.core.metaparse.AbstractCollectTableHandle;
import hyren.serv6.agent.job.biz.utils.CollectTableBeanUtil;
import hyren.serv6.commons.utils.TypeTransLength;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.UnloadType;
import hyren.serv6.base.entity.ColumnSplit;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.CollectTableColumnBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.bean.TbColTarTypeMapBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.xlstoxml.util.ColumnMeta;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/12/4 11:17")
public class DFCollectTableHandleParse extends AbstractCollectTableHandle {

    @SuppressWarnings("unchecked")
    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDataConfBean", desc = "", range = "")
    @Param(name = "collectTableBean", desc = "", range = "")
    @Return(desc = "", range = "")
    public TableBean generateTableInfo(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        TableBean tableBean = new TableBean();
        DataExtractionDef sourceData_extraction_def = CollectTableBeanUtil.getSourceData_extraction_def(collectTableBean.getData_extraction_def_list());
        tableBean.setFile_format(sourceData_extraction_def.getDbfile_format());
        tableBean.setIs_header(sourceData_extraction_def.getIs_header());
        tableBean.setRow_separator(sourceData_extraction_def.getRow_separator());
        tableBean.setColumn_separator(sourceData_extraction_def.getDatabase_separatorr());
        tableBean.setRoot_path(sourceData_extraction_def.getPlane_url());
        tableBean.setFile_code(sourceData_extraction_def.getDatabase_code());
        tableBean.setIs_archived(sourceData_extraction_def.getIs_archived());
        StringBuilder allColumns = new StringBuilder();
        StringBuilder allType = new StringBuilder();
        StringBuilder columnMetaInfo = new StringBuilder();
        StringBuilder colTypeMetaInfo = new StringBuilder();
        StringBuilder colLengthInfo = new StringBuilder();
        StringBuilder primaryKeyInfo = new StringBuilder();
        Map<String, Boolean> isZipperFieldInfo = new HashMap<>();
        String table_name = collectTableBean.getTable_name();
        List<String> cols = ColumnMeta.getColumnListByDictionary(table_name, sourceDataConfBean.getPlane_url());
        log.info(table_name + "#################数据字典列信息cols###############" + JsonUtil.toJson(cols));
        List<String> columnNameList = cols.stream().map(item -> StringUtil.split(item, STRSPLIT).get(0)).collect(Collectors.toList());
        Map<String, Object> dicColumnNameTypeMap = new HashMap<>();
        for (String col : cols) {
            List<String> split = StringUtil.split(col, STRSPLIT);
            dicColumnNameTypeMap.put(split.get(0), split.get(1));
        }
        List<CollectTableColumnBean> tableColumnBeanList = collectTableBean.getCollectTableColumnBeanList();
        Map<String, Object> columnNameTypeMap = new HashMap<>();
        for (CollectTableColumnBean tableColumnBean : tableColumnBeanList) {
            columnNameTypeMap.put(tableColumnBean.getColumn_name(), tableColumnBean.getColumn_type().toUpperCase());
        }
        List<String> columnNames = tableColumnBeanList.stream().map(CollectTableColumnBean::getColumn_name).collect(Collectors.toList());
        log.info(table_name + "#################数据字典列名称###############" + JsonUtil.toJson(columnNameList));
        log.info(table_name + "#################数据库保存列名称###############" + JsonUtil.toJson(columnNames));
        if (tableColumnBeanList.size() != cols.size()) {
            if (tableColumnBeanList.size() > cols.size()) {
                List<String> reduce = columnNames.stream().filter(item -> !columnNameList.contains(item)).collect(Collectors.toList());
                throw new AppSystemException("数据源编号:" + sourceDataConfBean.getDatasource_number() + ", Agent_id:" + sourceDataConfBean.getAgent_id() + ", 数据库采集任务名称:" + sourceDataConfBean.getTask_name() + ", 表名为" + table_name + ", 数据字典列缺失，缺失的列为：" + JsonUtil.toJson(reduce));
            } else {
                List<String> reduce = columnNameList.stream().filter(item -> !columnNames.contains(item)).collect(Collectors.toList());
                if (!reduce.isEmpty()) {
                    throw new AppSystemException("数据源编号:" + sourceDataConfBean.getDatasource_number() + ", Agent_id:" + sourceDataConfBean.getAgent_id() + ", 数据库采集任务名称:" + sourceDataConfBean.getTask_name() + ", 表名为" + table_name + ",数据字典列增加， 增加的列为：" + JsonUtil.toJson(reduce));
                }
            }
        }
        for (Map.Entry<String, Object> dicEntry : dicColumnNameTypeMap.entrySet()) {
            for (Map.Entry<String, Object> entry : columnNameTypeMap.entrySet()) {
                if (dicEntry.getKey().equals(entry.getKey())) {
                    String databaseColumnType = entry.getValue().toString();
                    String dicColumnType = dicEntry.getValue().toString();
                    if (!dicColumnType.equalsIgnoreCase(databaseColumnType)) {
                        throw new AppSystemException("数据源编号:" + sourceDataConfBean.getDatasource_number() + ", Agent_id:" + sourceDataConfBean.getAgent_id() + ", 数据库采集任务名称:" + sourceDataConfBean.getTask_name() + ", 表名为" + table_name + ", 数据字典列为：" + dicEntry.getKey() + ", 数据字典列类型发生变化,数据字典改变列:" + dicColumnType + "--》数据库保存原始列" + databaseColumnType);
                    }
                }
            }
        }
        if (!columnNameList.contains(Constant._HYREN_MD5_VAL) && IsFlag.Fou == IsFlag.ofEnumByCode(tableBean.getIs_archived()) && (StorageType.QuanLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.LiShiLaLian == StorageType.ofEnumByCode(collectTableBean.getStorage_type()))) {
            throw new AppSystemException("拉链存储选择全量拉链时数据字典中没有拉链字段时请选择转存！");
        }
        if (UnloadType.ZengLiangXieShu == UnloadType.ofEnumByCode(collectTableBean.getUnload_type()) && StorageType.ZengLiang != StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
            throw new AppSystemException("增量卸数方式只支持增量拉链不支持追加替换以及全量拉链存储方式");
        }
        List<TbColTarTypeMapBean> tbcol_srctgt_maps = collectTableBean.getTbColTarTypeMaps();
        List<CollectTableColumnBean> collectTableColumnBeanList = collectTableBean.getCollectTableColumnBeanList();
        Map<Long, String> tbColTarMap = new HashMap<>();
        Map<Long, List<TbColTarTypeMapBean>> tbColMap = new HashMap<>();
        if (!tbcol_srctgt_maps.isEmpty()) {
            tbColMap = tbcol_srctgt_maps.stream().collect(Collectors.groupingBy(TbColTarTypeMapBean::getDsl_id));
        }
        CollectTableBeanUtil.setColTarType(tbColTarMap, tbColMap, cols, AgentType.DBWenJian);
        log.info("========tbColTarMap===========" + JsonUtil.toJson(tbColTarMap));
        for (String col : cols) {
            List<String> colList = StringUtil.split(col, STRSPLIT);
            String colName = colList.get(0);
            String colType = colList.get(1);
            allColumns.append(colName).append(STRSPLIT);
            allType.append(colType).append(STRSPLIT);
            if (IsFlag.Shi.getCode().equals(tableBean.getIs_archived())) {
                if (!Constant.HYRENFIELD.contains(colName.toUpperCase())) {
                    columnMetaInfo.append(colName).append(STRSPLIT);
                    colTypeMetaInfo.append(colType).append(STRSPLIT);
                    colLengthInfo.append(TypeTransLength.getLength(colType)).append(STRSPLIT);
                    primaryKeyInfo.append(colList.get(2)).append(STRSPLIT);
                    boolean zipper_flag = true;
                    for (CollectTableColumnBean columnBean : collectTableColumnBeanList) {
                        if (columnBean.getColumn_name().equals(colName)) {
                            isZipperFieldInfo.put(colName, IsFlag.Shi.getCode().equals(columnBean.getIs_zipper_field()));
                            zipper_flag = false;
                            break;
                        }
                    }
                    if (zipper_flag) {
                        isZipperFieldInfo.put(colName, false);
                    }
                }
            } else if (IsFlag.Fou.getCode().equals(tableBean.getIs_archived())) {
                columnMetaInfo.append(colName).append(STRSPLIT);
                colTypeMetaInfo.append(colType).append(STRSPLIT);
                colLengthInfo.append(TypeTransLength.getLength(colType)).append(STRSPLIT);
                primaryKeyInfo.append(colList.get(2)).append(STRSPLIT);
            } else {
                throw new AppSystemException("错误的是否标识");
            }
        }
        if (colLengthInfo.length() > 0) {
            colLengthInfo.delete(colLengthInfo.length() - 1, colLengthInfo.length());
            allType.delete(allType.length() - 1, allType.length());
            allColumns.delete(allColumns.length() - 1, allColumns.length());
            colTypeMetaInfo.delete(colTypeMetaInfo.length() - 1, colTypeMetaInfo.length());
            columnMetaInfo.delete(columnMetaInfo.length() - 1, columnMetaInfo.length());
            primaryKeyInfo.delete(primaryKeyInfo.length() - 1, primaryKeyInfo.length());
        }
        Map<String, Object> parseJson = parseJson(collectTableBean);
        Map<String, Map<String, ColumnSplit>> splitIng = (Map<String, Map<String, ColumnSplit>>) parseJson.get("splitIng");
        Map<String, String> mergeIng = (Map<String, String>) parseJson.get("mergeIng");
        String colMeta = updateColumn(mergeIng, splitIng, columnMetaInfo, colTypeMetaInfo, colLengthInfo);
        columnMetaInfo.delete(0, columnMetaInfo.length()).append(colMeta);
        if (IsFlag.Shi.getCode().equals(tableBean.getIs_archived())) {
            columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_S_DATE);
            colTypeMetaInfo.append(STRSPLIT).append("char(8)");
            colLengthInfo.append(STRSPLIT).append("8");
            if (StorageType.ZengLiang.getCode().equals(collectTableBean.getStorage_type()) || StorageType.QuanLiang.getCode().equals(collectTableBean.getStorage_type())) {
                columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_E_DATE).append(STRSPLIT).append(Constant._HYREN_MD5_VAL);
                colTypeMetaInfo.append(STRSPLIT).append("char(8)").append(STRSPLIT).append("char(32)");
                colLengthInfo.append(STRSPLIT).append("8").append(STRSPLIT).append("32");
            }
            if (JobConstant.ISADDOPERATEINFO) {
                columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_OPER_DATE).append(STRSPLIT).append(Constant._HYREN_OPER_TIME).append(STRSPLIT).append(Constant._HYREN_OPER_PERSON);
                colTypeMetaInfo.append(STRSPLIT).append("char(10)").append(STRSPLIT).append("char(8)").append(STRSPLIT).append("char(4)");
                colLengthInfo.append(STRSPLIT).append("10").append(STRSPLIT).append("8").append(STRSPLIT).append("4");
            }
        } else {
            List<String> collectColumnList = collectTableColumnBeanList.stream().map(CollectTableColumnBean::getColumn_name).collect(Collectors.toList());
            addMd5Column(collectTableBean, columnMetaInfo, colTypeMetaInfo, colLengthInfo, collectColumnList, tableBean);
        }
        tableBean.setAllColumns(allColumns.toString());
        tableBean.setAllType(allType.toString());
        tableBean.setTbColTarMap(tbColTarMap);
        tableBean.setColLengthInfo(colLengthInfo.toString());
        tableBean.setColTypeMetaInfo(colTypeMetaInfo.toString());
        tableBean.setColumnMetaInfo(columnMetaInfo.toString());
        tableBean.setPrimaryKeyInfo(primaryKeyInfo.toString());
        tableBean.setParseJson(parseJson);
        tableBean.setIsZipperFieldInfo(isZipperFieldInfo);
        tableBean.setStorage_type(collectTableBean.getStorage_type());
        tableBean.setStorage_time(collectTableBean.getStorage_time());
        List<String> incrementColumnList = ColumnMeta.getIncrementColumnListByDictionary(collectTableBean.getTable_name(), sourceDataConfBean.getPlane_url());
        if (incrementColumnList != null && incrementColumnList.size() > 2) {
            tableBean.setInsertColumnInfo(incrementColumnList.get(0));
            tableBean.setUpdateColumnInfo(incrementColumnList.get(1));
            tableBean.setDeleteColumnInfo(incrementColumnList.get(2));
        }
        return tableBean;
    }

    private void addMd5Column(CollectTableBean collectTableBean, StringBuilder columnMetaInfo, StringBuilder colTypeMetaInfo, StringBuilder colLengthInfo, List<String> allColumnList, TableBean tableBean) {
        StorageType storageType = StorageType.ofEnumByCode(collectTableBean.getStorage_type());
        if ((storageType == StorageType.TiHuan || storageType == StorageType.ZhuiJia) && IsFlag.Shi == IsFlag.ofEnumByCode(collectTableBean.getIs_md5())) {
            if (!allColumnList.contains(Constant._HYREN_MD5_VAL) && !tableBean.getAppendMd5()) {
                columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_MD5_VAL);
                colTypeMetaInfo.append(STRSPLIT).append("char(32)");
                colLengthInfo.append(STRSPLIT).append("32");
                tableBean.setAppendMd5(true);
            }
        }
    }

    public static void main(String[] args) {
        String p = "D:\\data\\dd_data.json";
        List<String> cols = ColumnMeta.getColumnListByDictionary("agent_info", p);
        System.out.println(cols.size());
    }
}
