package hyren.serv6.agent.run.flink.consumer.service;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.run.flink.consumer.Column;
import hyren.serv6.agent.run.flink.consumer.JDBCData;
import hyren.serv6.base.codes.IsFlag;

public interface JDBCService {

    public int insert(List<JDBCData> jdbcDatas, Map<String, Object> afterData);

    public int update(List<JDBCData> jdbcDatas, Map<String, Object> beforeData, Map<String, Object> afterData);

    public int delete(List<JDBCData> jdbcDatas, Map<String, Object> beforeData);

    public void initTable(JDBCData jdbcData);

    default void createTable(DatabaseWrapper db, String afterTableName, List<Column> columns) {
        String sql = "CREATE TABLE " + afterTableName;
        StringJoiner columnJ = new StringJoiner(" , ", " ( ", " ) ");
        for (Column column : columns) {
            columnJ.add(column.getColumn_name() + " " + column.getColumn_tar_type() + (IsFlag.Shi.equals(column.getIs_primary_key()) ? " PRIMARY KEY " : ""));
        }
        sql += columnJ.toString();
        db.ExecDDL(sql);
        db.commit();
    }

    default void deleteTable(DatabaseWrapper db, String tableName) {
        String sql = "DROP TABLE " + tableName;
        db.ExecDDL(sql);
    }
}
