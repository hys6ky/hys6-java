package hyren.serv6.agent.job.biz.core.metaparse.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.core.metaparse.AbstractCollectTableHandle;
import hyren.serv6.agent.job.biz.utils.CollectTableBeanUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.UnloadType;
import hyren.serv6.base.entity.ColumnSplit;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.utils.SqlParamReplace;
import hyren.serv6.commons.utils.TypeTransLength;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.CollectTableColumnBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.bean.TbColTarTypeMapBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/12/4 11:17")
public class JdbcCollectTableHandleParse extends AbstractCollectTableHandle {

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDataConfBean", desc = "", range = "")
    @Param(name = "collectTableBean", desc = "", range = "")
    @Return(desc = "", range = "")
    public TableBean generateTableInfo(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        if (UnloadType.QuanLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
            return getFullAmountExtractTableBean(sourceDataConfBean, collectTableBean);
        } else if (UnloadType.ZengLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
            return getIncrementExtractTableBean(sourceDataConfBean, collectTableBean);
        } else {
            throw new AppSystemException("数据库抽取方式参数不正确");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDataConfBean", desc = "", range = "")
    @Param(name = "collectTableBean", desc = "", range = "")
    @Return(desc = "", range = "")
    private TableBean getIncrementExtractTableBean(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        TableBean tableBean = new TableBean();
        StringBuilder columnMetaInfo = new StringBuilder();
        StringBuilder allColumns = new StringBuilder();
        StringBuilder colTypeMetaInfo = new StringBuilder();
        StringBuilder allType = new StringBuilder();
        StringBuilder colLengthInfo = new StringBuilder();
        StringBuilder primaryKeyInfo = new StringBuilder();
        List<CollectTableColumnBean> collectTableColumnBeanList = collectTableBean.getCollectTableColumnBeanList();
        for (CollectTableColumnBean column : collectTableColumnBeanList) {
            columnMetaInfo.append(column.getColumn_name()).append(STRSPLIT);
            allColumns.append(column.getColumn_name()).append(STRSPLIT);
            colTypeMetaInfo.append(column.getColumn_type()).append(STRSPLIT);
            allType.append(column.getColumn_type()).append(STRSPLIT);
            colLengthInfo.append(TypeTransLength.getLength(column.getColumn_type(), sourceDataConfBean.getDatabase_type())).append(STRSPLIT);
            primaryKeyInfo.append(column.getIs_primary_key()).append(STRSPLIT);
        }
        columnMetaInfo.deleteCharAt(columnMetaInfo.length() - 1);
        allColumns.deleteCharAt(allColumns.length() - 1);
        colLengthInfo.deleteCharAt(colLengthInfo.length() - 1);
        colTypeMetaInfo.deleteCharAt(colTypeMetaInfo.length() - 1);
        allType.deleteCharAt(allType.length() - 1);
        primaryKeyInfo.deleteCharAt(primaryKeyInfo.length() - 1);
        Map<String, Object> sqlMap = JsonUtil.toObject(collectTableBean.getSql(), new TypeReference<Map<String, Object>>() {
        });
        Object update = sqlMap.get("update");
        if (null != update && StringUtil.isNotBlank(update.toString())) {
            if (!primaryKeyInfo.toString().contains(IsFlag.Shi.getCode())) {
                throw new AppSystemException("数据库增量卸数更新sql不为空，则必须要指定主键");
            }
        }
        tableBean.setAllColumns(allColumns.toString());
        tableBean.setAllType(allType.toString());
        tableBean.setColLengthInfo(colLengthInfo.toString());
        tableBean.setColTypeMetaInfo(colTypeMetaInfo.toString());
        tableBean.setColumnMetaInfo(columnMetaInfo.toString().toUpperCase());
        tableBean.setPrimaryKeyInfo(primaryKeyInfo.toString());
        tableBean.setDatabase_type(sourceDataConfBean.getDatabase_type());
        getSqlSearchColumn(sourceDataConfBean, collectTableBean, tableBean);
        return tableBean;
    }

    private void getSqlSearchColumn(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean, TableBean tableBean) {
        ResultSet resultSet = null;
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(CollectTableBeanUtil.setJdbcBean(sourceDataConfBean))) {
            String incrementSql = collectTableBean.getSql();
            Map<String, Object> incrementSqlObject = JsonUtil.toObject(incrementSql, new TypeReference<Map<String, Object>>() {
            });
            for (String key : incrementSqlObject.keySet()) {
                Object sql = incrementSqlObject.get(key);
                if (null != sql && StringUtil.isNotBlank(sql.toString())) {
                    StringBuilder columnMetaInfo = new StringBuilder();
                    sql = SqlParamReplace.replaceSqlParam(sql.toString(), collectTableBean.getSqlParam());
                    resultSet = getResultSet(sql.toString(), db);
                    ResultSetMetaData rsMetaData = resultSet.getMetaData();
                    for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                        String columnTmp = rsMetaData.getColumnName(i);
                        if (!columnTmp.equalsIgnoreCase("hyren_rn")) {
                            columnMetaInfo.append(columnTmp).append(STRSPLIT);
                        }
                    }
                    columnMetaInfo.deleteCharAt(columnMetaInfo.length() - 1);
                    if ("insert".equals(key)) {
                        tableBean.setInsertColumnInfo(columnMetaInfo.toString());
                    } else if ("delete".equals(key)) {
                        tableBean.setDeleteColumnInfo(columnMetaInfo.toString());
                    } else if ("update".equals(key)) {
                        tableBean.setUpdateColumnInfo(columnMetaInfo.toString());
                    } else {
                        throw new AppSystemException("增量数据采集不自持" + key + "操作");
                    }
                }
            }
        } catch (Exception e) {
            throw new AppSystemException("根据数据源信息和采集表信息得到卸数元信息失败！", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                log.error(String.format("e:%s", e));
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDataConfBean", desc = "", range = "")
    @Param(name = "collectTableBean", desc = "", range = "")
    @Return(desc = "", range = "")
    @SuppressWarnings("unchecked")
    private TableBean getFullAmountExtractTableBean(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        TableBean tableBean = new TableBean();
        ResultSet resultSet = null;
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(CollectTableBeanUtil.setJdbcBean(sourceDataConfBean))) {
            String collectSQL = getCollectSQL(collectTableBean, db, sourceDataConfBean.getDatabase_name());
            tableBean.setCollectSQL(collectSQL);
            if (collectSQL.contains(Constant.SQLDELIMITER)) {
                resultSet = getResultSet(StringUtil.split(collectSQL, Constant.SQLDELIMITER).get(0), db);
            } else {
                resultSet = getResultSet(collectSQL, db);
            }
            StringBuilder columnMetaInfo = new StringBuilder();
            StringBuilder allColumns = new StringBuilder();
            StringBuilder colTypeMetaInfo = new StringBuilder();
            StringBuilder allType = new StringBuilder();
            StringBuilder colLengthInfo = new StringBuilder();
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            int numberOfColumns = rsMetaData.getColumnCount();
            int[] typeArray = new int[numberOfColumns];
            for (int i = 1; i <= numberOfColumns; i++) {
                String columnTmp = rsMetaData.getColumnName(i);
                String[] names = columnTmp.split("\\.");
                columnTmp = names[names.length - 1];
                int columnType = rsMetaData.getColumnType(i);
                if (!columnTmp.equalsIgnoreCase("hyren_rn")) {
                    columnMetaInfo.append(columnTmp).append(STRSPLIT);
                    allColumns.append(columnTmp.toUpperCase()).append(STRSPLIT);
                }
                typeArray[i - 1] = columnType;
            }
            Map<String, String> tableColTypeAndLength = getTableColTypeAndLengthSql(resultSet);
            for (String key : tableColTypeAndLength.keySet()) {
                List<String> split = StringUtil.split(tableColTypeAndLength.get(key), "::");
                colTypeMetaInfo.append(split.get(0)).append(STRSPLIT);
                allType.append(split.get(0)).append(STRSPLIT);
                colLengthInfo.append(split.get(1)).append(STRSPLIT);
            }
            columnMetaInfo.deleteCharAt(columnMetaInfo.length() - 1);
            allColumns.deleteCharAt(allColumns.length() - 1);
            colLengthInfo.deleteCharAt(colLengthInfo.length() - 1);
            colTypeMetaInfo.deleteCharAt(colTypeMetaInfo.length() - 1);
            allType.deleteCharAt(allType.length() - 1);
            Map<String, Object> parseJson = parseJson(collectTableBean);
            Map<String, Map<String, ColumnSplit>> splitIng = (Map<String, Map<String, ColumnSplit>>) parseJson.get("splitIng");
            Map<String, String> mergeIng = (Map<String, String>) parseJson.get("mergeIng");
            String colMeta = updateColumn(mergeIng, splitIng, columnMetaInfo, colTypeMetaInfo, colLengthInfo);
            columnMetaInfo.delete(0, columnMetaInfo.length()).append(colMeta);
            if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_md5())) {
                columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_E_DATE).append(STRSPLIT).append(Constant._HYREN_MD5_VAL);
                colTypeMetaInfo.append(STRSPLIT).append("char(8)").append(STRSPLIT).append("char(32)");
                colLengthInfo.append(STRSPLIT).append("8").append(STRSPLIT).append("32");
            }
            if (JobConstant.ISADDOPERATEINFO) {
                columnMetaInfo.append(STRSPLIT).append(Constant._HYREN_OPER_DATE).append(STRSPLIT).append(Constant._HYREN_OPER_TIME).append(STRSPLIT).append(Constant._HYREN_OPER_PERSON);
                colTypeMetaInfo.append(STRSPLIT).append("char(10)").append(STRSPLIT).append("char(8)").append(STRSPLIT).append("char(4)");
                colLengthInfo.append(STRSPLIT).append("10").append(STRSPLIT).append("8").append(STRSPLIT).append("4");
            }
            StringBuilder primaryKeyInfo = new StringBuilder();
            Map<String, Boolean> isZipperFieldInfo = new HashMap<>();
            List<String> column_list = StringUtil.split(columnMetaInfo.toString(), STRSPLIT);
            List<CollectTableColumnBean> collectTableColumnBeanList = collectTableBean.getCollectTableColumnBeanList();
            collectTableColumnBeanList = collectTableColumnBeanList.stream().sorted(Comparator.comparing(CollectTableColumnBean::getColumn_id)).collect(Collectors.toList());
            List<TbColTarTypeMapBean> tbcol_srctgt_maps = collectTableBean.getTbColTarTypeMaps();
            Map<Long, String> tbColTarMap = new HashMap<>();
            Map<Long, List<TbColTarTypeMapBean>> tbColMap = new HashMap<>();
            if (!tbcol_srctgt_maps.isEmpty()) {
                tbColMap = tbcol_srctgt_maps.stream().collect(Collectors.groupingBy(TbColTarTypeMapBean::getDsl_id));
            }
            CollectTableBeanUtil.setColTarType(tbColTarMap, tbColMap, column_list, AgentType.ShuJuKu);
            StringBuilder sbColChName = new StringBuilder();
            for (String col : column_list) {
                boolean pk_flag = true;
                for (CollectTableColumnBean columnBean : collectTableColumnBeanList) {
                    if (columnBean.getColumn_name().equalsIgnoreCase(col)) {
                        primaryKeyInfo.append(columnBean.getIs_primary_key()).append(STRSPLIT);
                        pk_flag = false;
                        break;
                    }
                }
                if (pk_flag) {
                    primaryKeyInfo.append(IsFlag.Fou.getCode()).append(STRSPLIT);
                }
                boolean zipper_flag = true;
                for (CollectTableColumnBean columnBean : collectTableColumnBeanList) {
                    if (columnBean.getColumn_name().equalsIgnoreCase(col)) {
                        isZipperFieldInfo.put(col.toUpperCase(), IsFlag.Shi.getCode().equals(columnBean.getIs_zipper_field()));
                        zipper_flag = false;
                        break;
                    }
                }
                if (zipper_flag) {
                    isZipperFieldInfo.put(col.toUpperCase(), false);
                }
                for (CollectTableColumnBean columnBean : collectTableColumnBeanList) {
                    if (columnBean.getColumn_name().equalsIgnoreCase(col)) {
                        sbColChName.append(columnBean.getColumn_ch_name()).append(STRSPLIT);
                    }
                }
            }
            sbColChName.deleteCharAt(sbColChName.length() - 1);
            log.info("===========sbColChName=============" + sbColChName);
            primaryKeyInfo.deleteCharAt(primaryKeyInfo.length() - 1);
            tableBean.setAllColumns(allColumns.toString());
            tableBean.setAllType(allType.toString());
            tableBean.setColLengthInfo(colLengthInfo.toString());
            tableBean.setColTypeMetaInfo(colTypeMetaInfo.toString());
            tableBean.setColumnMetaInfo(columnMetaInfo.toString().toUpperCase());
            tableBean.setTypeArray(typeArray);
            tableBean.setParseJson(parseJson);
            tableBean.setPrimaryKeyInfo(primaryKeyInfo.toString());
            tableBean.setIsZipperFieldInfo(isZipperFieldInfo);
            tableBean.setDatabase_type(sourceDataConfBean.getDatabase_type());
            tableBean.setAllChColumns(sbColChName.toString());
        } catch (Exception e) {
            throw new AppSystemException("根据数据源信息和采集表信息得到卸数元信息失败！", e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return tableBean;
    }
}
