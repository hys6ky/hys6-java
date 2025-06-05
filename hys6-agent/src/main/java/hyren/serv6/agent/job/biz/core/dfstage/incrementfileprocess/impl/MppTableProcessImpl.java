package hyren.serv6.agent.job.biz.core.dfstage.incrementfileprocess.impl;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.core.dfstage.incrementfileprocess.TableProcessAbstract;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MppTableProcessImpl extends TableProcessAbstract {

    private final List<Object[]> addParamsPool = new ArrayList<>();

    private final List<Object[]> updateParamsPool = new ArrayList<>();

    private final StringBuilder deleteSql = new StringBuilder();

    private final DatabaseWrapper db;

    private final String insertSql;

    private final String updateSql;

    private final List<String> setColumnList;

    private final List<String> whereColumnList;

    private int deleteNum = 1;

    private final long dsl_id;

    public MppTableProcessImpl(TableBean tableBean, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean) {
        super(tableBean, collectTableBean);
        this.insertSql = getBatchInsertSql();
        this.updateSql = getBatchUpdateSql();
        this.dsl_id = dataStoreConfBean.getDsl_id();
        this.db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr());
        this.db.beginTrans();
        createTableIfNotExist();
        this.setColumnList = getSetColumnList();
        this.whereColumnList = getWhereColumnList();
        deleteSql.append("DELETE FROM ").append(collectTableBean.getStorage_table_name()).append(" WHERE ").append("(");
        Dbtype dbType = db.getDbtype();
        deleteColumnList = dbType.ofEscapedkey(deleteColumnList);
        for (String column : deleteColumnList) {
            deleteSql.append(column).append(",");
        }
        deleteSql.delete(deleteSql.length() - 1, deleteSql.length()).append(") IN (");
    }

    private void createTableIfNotExist() {
        String storageTableName = collectTableBean.getStorage_table_name();
        if (!db.isExistTable(storageTableName)) {
            StringBuilder create = new StringBuilder(1024);
            create.append("CREATE TABLE ");
            create.append(storageTableName);
            create.append("(");
            Dbtype dbType = db.getDbtype();
            List<String> tarTypes = StringUtil.split(tableBean.getTbColTarMap().get(this.dsl_id), Constant.METAINFOSPLIT);
            dictionaryColumnList = dbType.ofEscapedkey(dictionaryColumnList);
            for (int i = 0; i < dictionaryColumnList.size(); i++) {
                create.append(dictionaryColumnList.get(i)).append(" ").append(tarTypes.get(i));
                if (isPrimaryKeyMap.get(dictionaryColumnList.get(i))) {
                    create.append(" primary key");
                }
                create.append(",");
            }
            create.deleteCharAt(create.length() - 1);
            create.append(")");
            db.execute(create.toString());
        }
    }

    @Override
    public void dealData(Map<String, Map<String, Object>> valueList) {
        try {
            for (String operate : valueList.keySet()) {
                if ("insert".equals(operate)) {
                    Object[] object = new Object[insertColumnList.size()];
                    for (int i = 0; i < insertColumnList.size(); i++) {
                        object[i] = valueList.get(operate).get(insertColumnList.get(i));
                    }
                    addParamsPool.add(object);
                } else if ("update".equals(operate)) {
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
                deleteSql.append("DELETE FROM ").append(collectTableBean.getStorage_table_name()).append(" WHERE ").append("(");
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

    @Override
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
        StringBuilder updateSql = new StringBuilder();
        updateSql.append("UPDATE ").append(collectTableBean.getStorage_table_name()).append(" SET ");
        StringBuilder sb = new StringBuilder();
        sb.append(" WHERE ");
        updateColumnList = db.getDbtype().ofEscapedkey(updateColumnList);
        for (String updateColumn : updateColumnList) {
            if (!isPrimaryKeyMap.get(updateColumn)) {
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

    private String getBatchInsertSql() {
        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(collectTableBean.getStorage_table_name()).append(" (");
        StringBuilder sb = new StringBuilder();
        sb.append(" ) VALUES (");
        insertColumnList = db.getDbtype().ofEscapedkey(insertColumnList);
        for (String column : insertColumnList) {
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

    private List<String> getSetColumnList() {
        List<String> setColumnList = new ArrayList<>();
        for (String updateColumn : updateColumnList) {
            if (!isPrimaryKeyMap.get(updateColumn)) {
                setColumnList.add(updateColumn);
            }
        }
        return setColumnList;
    }

    private List<String> getWhereColumnList() {
        List<String> whereColumnList = new ArrayList<>();
        if (!updateColumnList.isEmpty()) {
            for (String updateColumn : updateColumnList) {
                if (isPrimaryKeyMap.get(updateColumn)) {
                    whereColumnList.add(updateColumn);
                }
            }
            if (whereColumnList.isEmpty()) {
                throw new AppSystemException("DB文件采集，采集增量数据，直接更新，没有指定主键");
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
