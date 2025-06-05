package hyren.serv6.agent.job.biz.core.objectstage.service.impl;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.core.objectstage.service.ObjectProcessAbstract;
import hyren.serv6.base.codes.CollectDataType;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.OperationType;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MppTableProcessImpl extends ObjectProcessAbstract {

    private final List<Object[]> addParamsPool = new ArrayList<>();

    private final List<Object[]> updateParamsPool = new ArrayList<>();

    private final StringBuilder deleteSql = new StringBuilder();

    private final DatabaseWrapper db;

    private final String insertSql;

    private final String updateSql;

    protected Map<String, Boolean> isPrimaryKeyMap;

    private final List<String> deleteColumnList;

    private final List<String> setColumnList;

    private final List<String> whereColumnList;

    private int deleteNum = 1;

    private final String dsl_name;

    public MppTableProcessImpl(TableBean tableBean, ObjectTableBean objectTableBean, DataStoreConfBean dataStoreConfBean) {
        super(tableBean, objectTableBean);
        if (isZipperKeyMap.isEmpty()) {
            for (String column : selectColumnList) {
                isZipperKeyMap.put(column, false);
            }
        }
        this.dsl_name = dataStoreConfBean.getDsl_name();
        this.insertSql = getBatchInsertSql();
        this.whereColumnList = getWhereColumnList();
        this.updateSql = getBatchUpdateSql();
        this.isPrimaryKeyMap = getPrimaryKeyMap(dataStoreConfBean.getSortAdditInfoFieldMap());
        this.db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr());
        this.db.beginTrans();
        createTableIfNotExist();
        this.deleteColumnList = getDeleteColumnList(isZipperKeyMap);
        this.setColumnList = getSetColumnList(isZipperKeyMap);
        deleteSql.append("DELETE FROM ").append(objectTableBean.getHyren_name()).append(" WHERE ").append("(");
        for (String column : deleteColumnList) {
            deleteSql.append(column).append(",");
        }
        deleteSql.delete(deleteSql.length() - 1, deleteSql.length()).append(") IN (");
    }

    @Override
    public void parserFileToTable(String readFile) {
        if (CollectDataType.JSON.getCode().equals(objectTableBean.getCollect_data_type())) {
            parseJsonFileToTable(readFile);
        } else if (CollectDataType.XML.getCode().equals(objectTableBean.getCollect_data_type())) {
            throw new AppSystemException("暂不支持xml半结构化文件采集");
        } else {
            throw new AppSystemException("半结构化对象采集入库只支持JSON和XML两种格式");
        }
    }

    private void parseJsonFileToTable(String readFile) {
        String lineValue;
        String code = DataBaseCode.ofValueByCode(objectTableBean.getDatabase_code());
        long num = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(readFile)), code))) {
            while ((lineValue = br.readLine()) != null) {
                num++;
                List<Map<String, Object>> listTiledAttributes = getListTiledAttributes(lineValue, num);
                for (Map<String, Object> map : listTiledAttributes) {
                    Map<String, Map<String, Object>> dealMap = new HashMap<>();
                    map.put(Constant._HYREN_S_DATE, etlDate);
                    map.put(Constant._HYREN_E_DATE, Constant._MAX_DATE_8);
                    map.put(Constant._HYREN_MD5_VAL, Constant._HYREN_MD5_VAL);
                    if (OperationType.INSERT.getCode().equals(handleTypeMap.get(map.get(tableBean.getOperate_column()).toString()))) {
                        dealMap.put("insert", map);
                    } else if (OperationType.UPDATE.getCode().equals(handleTypeMap.get(map.get(tableBean.getOperate_column()).toString()))) {
                        dealMap.put("update", map);
                    } else if (OperationType.DELETE.getCode().equals(handleTypeMap.get(map.get(tableBean.getOperate_column()).toString()))) {
                        dealMap.put("delete", map);
                    } else {
                        throw new AppSystemException("不支持的操作类型" + map.get(tableBean.getOperate_column()));
                    }
                    dealData(dealMap);
                }
            }
            excute();
        } catch (Exception e) {
            throw new AppSystemException("解析半结构化对象文件报错", e);
        }
    }

    private List<String> getDeleteColumnList(Map<String, Boolean> isZipperKeyMap) {
        List<String> delColumnList = new ArrayList<>();
        for (String column : selectColumnList) {
            if (isZipperKeyMap.get(column)) {
                delColumnList.add(column);
            }
        }
        if (delColumnList.size() == 0) {
            delColumnList = selectColumnList;
        }
        return delColumnList;
    }

    private Map<String, Boolean> getPrimaryKeyMap(Map<String, Map<Integer, String>> additInfoFieldMap) {
        Map<String, Boolean> pMap = new HashMap<>();
        if (additInfoFieldMap != null && !additInfoFieldMap.isEmpty()) {
            for (String dsla_storelayer : additInfoFieldMap.keySet()) {
                if (StoreLayerAdded.ZhuJian.getCode().equals(dsla_storelayer)) {
                    List<String> primaryColumnList = new ArrayList<>(additInfoFieldMap.get(dsla_storelayer).values());
                    for (String column : metaColumnList) {
                        if (primaryColumnList.contains(column)) {
                            pMap.put(column, true);
                        } else {
                            pMap.put(column, false);
                        }
                    }
                }
            }
        }
        if (pMap.isEmpty()) {
            for (String column : metaColumnList) {
                pMap.put(column, false);
            }
        }
        return pMap;
    }

    private void createTableIfNotExist() {
        if (!db.isExistTable(objectTableBean.getHyren_name())) {
            StringBuilder create = new StringBuilder(1024);
            create.append("CREATE TABLE ");
            create.append(objectTableBean.getHyren_name());
            create.append("(");
            for (int i = 0; i < metaColumnList.size(); i++) {
                create.append(metaColumnList.get(i)).append(" ").append(metaTypeList.get(i));
                if (isPrimaryKeyMap.get(metaColumnList.get(i))) {
                    create.append(" primary key");
                }
                create.append(",");
            }
            create.deleteCharAt(create.length() - 1);
            create.append(")");
            db.execute(create.toString());
        }
    }

    public void dealData(Map<String, Map<String, Object>> valueList) {
        try {
            for (String operate : valueList.keySet()) {
                if ("insert".equals(operate)) {
                    Object[] object = new Object[metaColumnList.size()];
                    for (int i = 0; i < metaColumnList.size(); i++) {
                        object[i] = valueList.get(operate).get(metaColumnList.get(i));
                    }
                    addParamsPool.add(object);
                } else if ("update".equals(operate)) {
                    if (whereColumnList.isEmpty()) {
                        throw new AppSystemException("半结构对象采集存储层选择" + dsl_name + "有更新操作，但没有选择主键");
                    }
                    Object[] object = new Object[setColumnList.size() + whereColumnList.size()];
                    for (int i = 0; i < setColumnList.size(); i++) {
                        object[i] = valueList.get(operate).get(setColumnList.get(i));
                    }
                    for (int i = 0; i < whereColumnList.size(); i++) {
                        object[setColumnList.size() + i] = valueList.get(operate).get(whereColumnList.get(i));
                    }
                    updateParamsPool.add(object);
                } else if ("delete".equals(operate)) {
                    deleteNum++;
                    deleteSql.append("(");
                    for (String column : deleteColumnList) {
                        deleteSql.append(getDeleteValue(valueList.get(operate).get(column))).append(",");
                    }
                    deleteSql.delete(deleteSql.length() - 1, deleteSql.length());
                    deleteSql.append(")").append(",");
                } else {
                    throw new AppSystemException("增量数据采集不自持" + operate + "操作");
                }
            }
            if (deleteNum % 1000 == 0) {
                deleteSql.delete(deleteSql.length() - 1, deleteSql.length()).append(")");
                db.execute(deleteSql.toString());
                deleteSql.delete(0, deleteSql.length());
                deleteSql.append("DELETE FROM ").append(objectTableBean.getHyren_name()).append(" WHERE ").append("(");
                for (String column : deleteColumnList) {
                    deleteSql.append(column).append(",");
                }
                deleteSql.delete(deleteSql.length() - 1, deleteSql.length()).append(") IN (");
                deleteNum = 1;
            }
            if (updateParamsPool.size() != 0 && updateParamsPool.size() % 5000 == 0) {
                if (deleteNum > 1) {
                    deleteSql.delete(deleteSql.length() - 1, deleteSql.length()).append(")");
                    db.execute(deleteSql.toString());
                    deleteNum = 1;
                }
                db.execBatch(updateSql, updateParamsPool);
                updateParamsPool.clear();
            }
            if (addParamsPool.size() != 0 && addParamsPool.size() % 5000 == 0) {
                if (updateParamsPool.size() > 0) {
                    db.execBatch(updateSql, updateParamsPool);
                    updateParamsPool.clear();
                }
                db.execBatch(insertSql, addParamsPool);
                addParamsPool.clear();
            }
        } catch (Exception e) {
            if (db != null)
                db.rollback();
            throw new AppSystemException("Mpp数据库增量模式直接更新库失败", e);
        }
    }

    public void excute() {
        try {
            if (deleteNum > 1) {
                deleteSql.delete(deleteSql.length() - 1, deleteSql.length()).append(")");
                db.execute(deleteSql.toString());
                deleteNum = 1;
            }
            if (updateParamsPool.size() > 0) {
                db.execBatch(updateSql, updateParamsPool);
            }
            if (addParamsPool.size() > 0) {
                db.execBatch(insertSql, addParamsPool);
            }
        } catch (Exception e) {
            if (db != null)
                db.rollback();
            throw new AppSystemException("Mpp数据库增量模式直接更新库失败", e);
        }
    }

    private String getBatchUpdateSql() {
        if (!this.whereColumnList.isEmpty()) {
            StringBuilder updateSql = new StringBuilder();
            updateSql.append("UPDATE ").append(objectTableBean.getHyren_name()).append(" SET ");
            StringBuilder sb = new StringBuilder();
            sb.append(" WHERE ");
            for (String updateColumn : metaColumnList) {
                if (!isZipperKeyMap.get(updateColumn)) {
                    updateSql.append(updateColumn).append(" = ?,");
                } else {
                    sb.append(updateColumn).append(" = ? and ");
                }
            }
            updateSql.delete(updateSql.length() - 1, updateSql.length());
            sb.delete(sb.length() - 4, sb.length());
            updateSql.append(sb);
            return updateSql.toString();
        }
        return "";
    }

    private String getBatchInsertSql() {
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(objectTableBean.getHyren_name()).append(" (");
        StringBuilder sb = new StringBuilder();
        sb.append(" ) VALUES (");
        for (String column : metaColumnList) {
            insertSql.append(column).append(",");
            sb.append("?").append(",");
        }
        insertSql.delete(insertSql.length() - 1, insertSql.length());
        sb.delete(sb.length() - 1, sb.length()).append(" ) ");
        insertSql.append(sb);
        return insertSql.toString();
    }

    @Override
    public void close() {
        db.commit();
        db.close();
    }

    private List<String> getSetColumnList(Map<String, Boolean> isZipperKeyMap) {
        List<String> setColumnList = new ArrayList<>();
        for (String updateColumn : metaColumnList) {
            if (isZipperKeyMap.get(updateColumn) != null && !isZipperKeyMap.get(updateColumn)) {
                setColumnList.add(updateColumn);
            }
        }
        return setColumnList;
    }

    private List<String> getWhereColumnList() {
        List<String> whereColumnList = new ArrayList<>();
        for (String updateColumn : metaColumnList) {
            if (isZipperKeyMap.get(updateColumn) != null && isZipperKeyMap.get(updateColumn)) {
                whereColumnList.add(updateColumn);
            }
        }
        return whereColumnList;
    }

    private Object getDeleteValue(Object value) {
        if (value instanceof String) {
            String strData = (String) value;
            return "'" + strData + "'";
        } else {
            return value;
        }
    }
}
