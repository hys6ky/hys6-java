package hyren.serv6.agent.run.flink.consumer.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import fd.ng.core.utils.DateUtil;
import hyren.serv6.agent.run.flink.consumer.Column;
import hyren.serv6.agent.run.flink.consumer.JDBCData;
import hyren.serv6.base.codes.IsFlag;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class F3JDBCServiceImpl implements JDBCService {

    private static String START_DATE_DT = "HYREN_S_DATE";

    private static String END_DATE_DT = "HYREN_E_DATE";

    private static String DATE_DT_COLUMN = "VARCHAR(8)";

    @Override
    public int insert(List<JDBCData> jdbcDatas, Map<String, Object> afterData) {
        int num = 0;
        for (JDBCData jdbcData : jdbcDatas) {
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
            columnJ.add(START_DATE_DT);
            columnValueJ.add("?");
            params.add(DateUtil.getSysDate());
            sql += columnJ + " VALUES " + columnValueJ;
            try {
                jdbcData.getDb().execute(sql, params);
                num++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return num;
    }

    @Override
    public int update(List<JDBCData> jdbcDatas, Map<String, Object> beforeData, Map<String, Object> afterData) {
        int num = 0;
        num += this.delete(jdbcDatas, beforeData);
        num += this.insert(jdbcDatas, afterData);
        return num;
    }

    @Override
    public int delete(List<JDBCData> jdbcDatas, Map<String, Object> beforeData) {
        int num = 0;
        for (JDBCData jdbcData : jdbcDatas) {
            String tableName = jdbcData.getAfter_table_name();
            List<Column> columns = jdbcData.getColumns();
            String sql = "UPDATE " + tableName + " SET ";
            List<Object> params = new ArrayList<Object>();
            StringJoiner setJ = new StringJoiner(" , ");
            StringJoiner whereJ = new StringJoiner(" AND ");
            setJ.add(END_DATE_DT + " = ?");
            params.add(DateUtil.getSysDate());
            for (Column column : columns) {
                if (beforeData.get(column.getColumn_name()) == null) {
                    whereJ.add(column.getColumn_name() + " IS NULL");
                } else {
                    whereJ.add(column.getColumn_name() + " = ?");
                    params.add(beforeData.get(column.getColumn_name()));
                }
            }
            whereJ.add(END_DATE_DT + " IS NULL ");
            sql += setJ + " WHERE " + whereJ;
            try {
                jdbcData.getDb().execute(sql, params);
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
            ArrayList<Column> columns = new ArrayList<>(jdbcData.getColumns());
            columns.stream().forEach(c -> c.setIs_primary_key(IsFlag.Fou));
            columns.add(new Column(START_DATE_DT, null, DATE_DT_COLUMN, IsFlag.Fou));
            columns.add(new Column(END_DATE_DT, null, DATE_DT_COLUMN, IsFlag.Fou));
            createTable(jdbcData.getDb(), jdbcData.getAfter_table_name(), columns);
            log.info("创建表：" + jdbcData.getAfter_table_name());
            jdbcData.setExist(false);
        } else {
            log.info("表已存在：" + jdbcData.getAfter_table_name());
            jdbcData.setExist(true);
        }
    }
}
