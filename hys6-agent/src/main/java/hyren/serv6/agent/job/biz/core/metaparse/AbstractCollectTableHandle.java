package hyren.serv6.agent.job.biz.core.metaparse;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.utils.ColumnTool;
import hyren.serv6.commons.utils.TypeTransLength;
import hyren.serv6.base.codes.CharSplitType;
import hyren.serv6.base.codes.CleanType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.ColumnMerge;
import hyren.serv6.base.entity.ColumnSplit;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.SqlParamReplace;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.CollectTableColumnBean;
import hyren.serv6.commons.utils.agent.bean.ColumnCleanBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.xlstoxml.Platform;
import lombok.extern.slf4j.Slf4j;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

@Slf4j
public abstract class AbstractCollectTableHandle implements CollectTableHandle {

    protected static final String STRSPLIT = Constant.METAINFOSPLIT;

    protected ResultSet getResultSet(String collectSQL, DatabaseWrapper db) {
        ResultSet columnSet;
        try {
            String exeSql = String.format("SELECT * FROM ( %s ) HYREN_WHERE_ALIAS WHERE 1 = 2", collectSQL);
            columnSet = db.queryGetResultSet(exeSql);
        } catch (Exception e) {
            throw new AppSystemException("获取ResultSet异常", e);
        }
        return columnSet;
    }

    protected String getCollectSQL(CollectTableBean collectTableBean, DatabaseWrapper db, String database_name) {
        String collectSQL;
        if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_user_defined())) {
            collectSQL = collectTableBean.getSql();
        } else {
            if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_parallel())) {
                if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_customize_sql())) {
                    if (!StringUtil.isEmpty(collectTableBean.getSql())) {
                        collectSQL = getCollectSqlAddWhere(collectTableBean.getPage_sql(), collectTableBean.getSql());
                    } else {
                        collectSQL = collectTableBean.getPage_sql();
                    }
                } else if (IsFlag.Fou.getCode().equals(collectTableBean.getIs_customize_sql())) {
                    collectSQL = getCollectSqlByColumn(collectTableBean, db, database_name);
                } else {
                    throw new AppSystemException("是否标识参数错误");
                }
            } else if (IsFlag.Fou.getCode().equals(collectTableBean.getIs_parallel())) {
                collectSQL = getCollectSqlByColumn(collectTableBean, db, database_name);
            } else {
                throw new AppSystemException("是否标识参数错误");
            }
        }
        collectSQL = SqlParamReplace.replaceSqlParam(collectSQL, collectTableBean.getSqlParam());
        log.info("采集要执行的sql为" + collectSQL);
        return collectSQL;
    }

    private String getCollectSqlByColumn(CollectTableBean collectTableBean, DatabaseWrapper db, String database_name) {
        String tableName = collectTableBean.getTable_name();
        Set<String> collectColumnNames = ColumnTool.getCollectColumnName(collectTableBean.getCollectTableColumnBeanList());
        String collectSql = db.getDbtype().ofKeyLableSql(tableName, collectColumnNames, database_name);
        return getCollectSqlAddWhere(collectSql, collectTableBean.getSql());
    }

    private String getCollectSqlAddWhere(String collectSql, String filter) {
        StringBuilder addWhereSql = new StringBuilder();
        if (!StringUtil.isEmpty(filter)) {
            for (String sql : StringUtil.split(collectSql, Constant.SQLDELIMITER)) {
                addWhereSql.append("SELECT * FROM (").append(sql).append(")").append(" hyren_tmp_where ").append(" WHERE ").append(filter).append(Constant.SQLDELIMITER);
            }
            addWhereSql.delete(addWhereSql.length() - Constant.SQLDELIMITER.length(), addWhereSql.length());
            return addWhereSql.toString();
        } else {
            return collectSql;
        }
    }

    protected Map<String, String> getTableColTypeAndLengthSql(ResultSet columnSet) {
        Map<String, String> map = new LinkedHashMap<>();
        try {
            ResultSetMetaData rsMetaData = columnSet.getMetaData();
            int numberOfColumns = rsMetaData.getColumnCount();
            for (int i = 1; i <= numberOfColumns; i++) {
                String colType = Platform.getColType(rsMetaData.getColumnType(i), rsMetaData.getColumnTypeName(i), rsMetaData.getPrecision(i), rsMetaData.getScale(i), rsMetaData.getColumnDisplaySize(i));
                log.info("列名称是: " + rsMetaData.getColumnName(i).toLowerCase() + "  数据库类型是: " + colType + "  Types的数据类型: " + rsMetaData.getColumnType(i));
                map.put(rsMetaData.getColumnName(i).toLowerCase(), colType + "::" + TypeTransLength.getLength(colType));
            }
        } catch (Exception e) {
            throw new AppSystemException("使用sql获取每个字段的长度，类型，精度失败", e);
        }
        return map;
    }

    protected Map<String, Object> parseJson(CollectTableBean collectTableBean) {
        Map<String, Object> all = new HashMap<>();
        Map<String, Map<String, String>> deleSpecialSpace = new HashMap<>();
        Map<String, String> strFilling = new HashMap<>();
        Map<String, String> strDateing = new HashMap<>();
        Map<String, Map<String, ColumnSplit>> splitIng = new HashMap<>();
        Map<String, String> mergeIng = new LinkedHashMap<>();
        Map<String, String> codeIng = new HashMap<>();
        Map<String, String> Triming = new HashMap<>();
        Map<String, Map<Integer, String>> ordering = new HashMap<>();
        List<CollectTableColumnBean> collectTableColumnBeanList = collectTableBean.getCollectTableColumnBeanList();
        if (!collectTableColumnBeanList.isEmpty()) {
            for (CollectTableColumnBean collectTableColumnBean : collectTableColumnBeanList) {
                String column_name_up = collectTableColumnBean.getColumn_name().toUpperCase();
                Map<String, String> replaceMap = new HashMap<>();
                String order = collectTableColumnBean.getTc_or();
                Map<Integer, String> changeKeyValue = changeKeyValue(order);
                ordering.put(column_name_up, changeKeyValue);
                List<ColumnCleanBean> column_clean_list = collectTableColumnBean.getColumnCleanBeanList();
                if (!column_clean_list.isEmpty()) {
                    for (ColumnCleanBean columnCleanBean : column_clean_list) {
                        if (CleanType.ZiFuTiHuan.getCode().equals(columnCleanBean.getClean_type())) {
                            replaceMap.put(StringUtil.unicode2String(columnCleanBean.getField()), StringUtil.unicode2String(columnCleanBean.getReplace_feild()));
                            deleSpecialSpace.put(column_name_up, replaceMap);
                        } else if (CleanType.ZiFuBuQi.getCode().equals(columnCleanBean.getClean_type())) {
                            String filling = columnCleanBean.getFilling_length() + STRSPLIT + columnCleanBean.getFilling_type() + STRSPLIT + StringUtil.unicode2String(columnCleanBean.getCharacter_filling());
                            strFilling.put(column_name_up, filling);
                        } else if (CleanType.ShiJianZhuanHuan.getCode().equals(columnCleanBean.getClean_type())) {
                            String dateConvert = columnCleanBean.getConvert_format() + STRSPLIT + columnCleanBean.getOld_format();
                            strDateing.put(column_name_up, dateConvert);
                        } else if (CleanType.ZiFuTrim.getCode().equals(columnCleanBean.getClean_type())) {
                            Triming.put(column_name_up, "trim");
                        } else if (CleanType.MaZhiZhuanHuan.getCode().equals(columnCleanBean.getClean_type())) {
                            codeIng.put(column_name_up, columnCleanBean.getCodeTransform());
                        } else if (CleanType.ZiFuChaiFen.getCode().equals(columnCleanBean.getClean_type())) {
                            Map<String, ColumnSplit> map = new HashMap<>();
                            List<ColumnSplit> column_split_list = columnCleanBean.getColumn_split_list();
                            for (ColumnSplit column_split : column_split_list) {
                                if (CharSplitType.ZhiDingFuHao.getCode().equals(column_split.getSplit_type())) {
                                    column_split.setSplit_sep(StringUtil.unicode2String(column_split.getSplit_sep()));
                                }
                                map.put(column_split.getCol_name(), column_split);
                            }
                            splitIng.put(column_name_up, map);
                        } else {
                            throw new AppSystemException("请选择正确的清洗方式");
                        }
                    }
                }
            }
        }
        List<ColumnMerge> column_merge_list = collectTableBean.getColumn_merge_list();
        if (column_merge_list != null && column_merge_list.size() > 0) {
            for (ColumnMerge column_merge : column_merge_list) {
                mergeIng.put(column_merge.getCol_name() + STRSPLIT + column_merge.getCol_type(), column_merge.getOld_name());
            }
        }
        all.put("deleSpecialSpace", deleSpecialSpace);
        all.put("strFilling", strFilling);
        all.put("dating", strDateing);
        all.put("splitIng", splitIng);
        all.put("mergeIng", mergeIng);
        all.put("codeIng", codeIng);
        all.put("Triming", Triming);
        all.put("ordering", ordering);
        return all;
    }

    private Map<Integer, String> changeKeyValue(String order) {
        Map<Integer, String> map = new HashMap<>();
        if (!StringUtil.isEmpty(order)) {
            Map<String, Object> jsonOrder = JsonUtil.toObject(order, new TypeReference<Map<String, Object>>() {
            });
            Set<String> jsonSet = jsonOrder.keySet();
            for (String key : jsonSet) {
                map.put(Integer.parseInt(jsonOrder.get(key).toString()), key);
            }
        }
        return map;
    }

    protected String updateColumn(Map<String, String> mergeIng, Map<String, Map<String, ColumnSplit>> splitIng, StringBuilder columns, StringBuilder colType, StringBuilder lengths) {
        if (!mergeIng.isEmpty()) {
            for (String key : mergeIng.keySet()) {
                List<String> split = StringUtil.split(key, STRSPLIT);
                columns.append(STRSPLIT).append(split.get(0));
                colType.append(STRSPLIT).append(split.get(1));
                lengths.append(STRSPLIT).append(TypeTransLength.getLength(split.get(1)));
            }
        }
        String columnMate = columns.toString();
        if (!splitIng.isEmpty()) {
            for (String key : splitIng.keySet()) {
                StringBuilder newColumn = new StringBuilder();
                StringBuilder newColumnType = new StringBuilder();
                StringBuilder newColumnLength = new StringBuilder();
                Map<String, ColumnSplit> map = splitIng.get(key);
                if (map != null) {
                    newColumn.append(key).append(STRSPLIT);
                    int findColIndex = ColumnTool.findColIndex(columnMate, key, STRSPLIT);
                    for (String newName : map.keySet()) {
                        ColumnSplit column_split = map.get(newName);
                        newColumn.append(newName).append(STRSPLIT);
                        newColumnType.append(column_split.getCol_type()).append(STRSPLIT);
                        newColumnLength.append(TypeTransLength.getLength(column_split.getCol_type())).append(STRSPLIT);
                    }
                    newColumn.deleteCharAt(newColumn.length() - 1);
                    newColumnType.deleteCharAt(newColumnType.length() - 1);
                    newColumnLength.deleteCharAt(newColumnLength.length() - 1);
                    int searchIndex = ColumnTool.searchIndex(colType.toString(), findColIndex, STRSPLIT);
                    int lenIndex = ColumnTool.searchIndex(lengths.toString(), findColIndex, STRSPLIT);
                    if (searchIndex != -1) {
                        colType.insert(searchIndex, STRSPLIT + newColumnType.toString());
                    } else {
                        colType.append(STRSPLIT).append(newColumnType.toString());
                    }
                    if (lenIndex != -1) {
                        lengths.insert(lenIndex, STRSPLIT + newColumnLength.toString());
                    } else {
                        lengths.append(STRSPLIT).append(newColumnLength.toString());
                    }
                    columnMate = StringUtil.replace(columnMate.toUpperCase(), key.toUpperCase(), newColumn.toString().toUpperCase());
                }
            }
        }
        return columnMate;
    }

    public static void main(String[] args) {
        String aaa = "select aaa,bbb,ccc,#{ccc} from item where aaa='ccc' and   i_rec_start_date   =   #{i_rec_start_date}";
        String aa = aaa.replaceAll("\\s+?((?!\\s).)+?\\s+?=\\s+?#\\{.*\\}", " 1=2");
        System.out.println(aa);
    }
}
