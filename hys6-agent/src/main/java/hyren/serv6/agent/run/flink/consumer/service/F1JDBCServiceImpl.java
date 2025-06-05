package hyren.serv6.agent.run.flink.consumer.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.run.flink.consumer.Column;
import hyren.serv6.agent.run.flink.consumer.JDBCData;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class F1JDBCServiceImpl implements JDBCService {

    @Override
    public int insert(List<JDBCData> jdbcDatas, Map<String, Object> afterData) {
        int num = 0;
        for (JDBCData jdbcData : jdbcDatas) {
            DatabaseWrapper db = jdbcData.getDb();
            String tableName = jdbcData.getAfter_table_name();
            List<Column> columns = jdbcData.getColumns();
            String sql = "INSERT INTO " + tableName;
            List<Object> params = new ArrayList<Object>();
            StringJoiner columnJ = new StringJoiner(", ", " ( ", ") ");
            StringJoiner columnValueJ = new StringJoiner(", ", " ( ", ") ");
            for (Column column : columns) {
                columnJ.add(column.getColumn_name());
                columnValueJ.add("?");
                params.add(afterData.get(column.getColumn_name()));
            }
            sql += columnJ.toString() + " VALUES " + columnValueJ;
            db.execute(sql, params);
            db.commit();
            num++;
        }
        return num;
    }

    @Override
    public int update(List<JDBCData> jdbcDatas, Map<String, Object> beforeData, Map<String, Object> afterData) {
        int num = 0;
        for (JDBCData jdbcData : jdbcDatas) {
            DatabaseWrapper db = jdbcData.getDb();
            String tableName = jdbcData.getAfter_table_name();
            List<Column> columns = jdbcData.getColumns();
            String sql = "UPDATE " + tableName + " SET ";
            List<Object> paramsSet = new ArrayList<Object>();
            List<Object> paramsWhere = new ArrayList<Object>();
            StringJoiner setJ = new StringJoiner(" , ");
            StringJoiner whereJ = new StringJoiner(" AND ");
            for (Column column : columns) {
                setJ.add(column.getColumn_name() + " = ?");
                paramsSet.add(afterData.get(column.getColumn_name()));
                if (beforeData.get(column.getColumn_name()) == null) {
                    whereJ.add(column.getColumn_name() + " IS NULL ");
                } else {
                    whereJ.add(column.getColumn_name() + " = ?");
                    paramsWhere.add(beforeData.get(column.getColumn_name()));
                }
            }
            sql += setJ + " WHERE " + whereJ;
            paramsSet.addAll(paramsWhere);
            try {
                db.execute(sql, paramsSet);
                num++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return num;
    }

    @Override
    public int delete(List<JDBCData> jdbcDatas, Map<String, Object> beforeData) {
        int num = 0;
        for (JDBCData jdbcData : jdbcDatas) {
            DatabaseWrapper db = jdbcData.getDb();
            String tableName = jdbcData.getAfter_table_name();
            List<Column> columns = jdbcData.getColumns();
            String sql = "DELETE FROM " + tableName + " WHERE ";
            List<Object> params = new ArrayList<Object>();
            StringJoiner whereJ = new StringJoiner(" AND ");
            for (Column column : columns) {
                if (beforeData.get(column.getColumn_name()) == null) {
                    whereJ.add(column.getColumn_name() + " IS NULL ");
                } else {
                    whereJ.add(column.getColumn_name() + " = ?");
                    params.add(beforeData.get(column.getColumn_name()));
                }
            }
            sql += whereJ;
            try {
                db.execute(sql, params);
                num++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return num;
    }

    @Override
    public void initTable(JDBCData jdbcData) {
        if (!jdbcData.getDb().isExistTable(jdbcData.getAfter_table_name())) {
            createTable(jdbcData.getDb(), jdbcData.getAfter_table_name(), jdbcData.getColumns());
            log.info("创建表：" + jdbcData.getAfter_table_name());
            jdbcData.setExist(false);
        } else {
            log.info("表已存在：" + jdbcData.getAfter_table_name());
            jdbcData.setExist(true);
        }
    }
}
