package hyren.serv6.agent.run.flink.consumer.service;

import java.util.List;
import java.util.Map;
import org.apache.poi.util.LittleEndian.BufferUnderrunException;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.run.flink.consumer.JDBCData;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.JDBCBean;

public class JDBCManager {

    private static JDBCService f1JdbcService = new F1JDBCServiceImpl();

    private static JDBCService f2JdbcService = new F2JDBCServiceImpl();

    private static JDBCService f3JdbcService = new F3JDBCServiceImpl();

    public static int insert(StorageType storageType, List<JDBCData> jdbcDatas, Map<String, Object> afterData) throws Exception {
        switch(storageType) {
            case TiHuan:
                return f1JdbcService.insert(jdbcDatas, afterData);
            case UpSet:
                return f2JdbcService.insert(jdbcDatas, afterData);
            case LiShiLaLian:
                return f3JdbcService.insert(jdbcDatas, afterData);
            default:
                throw new Exception("暂不支持该类型操作");
        }
    }

    public static int update(StorageType storageType, List<JDBCData> jdbcDatas, Map<String, Object> beforeData, Map<String, Object> afterData) throws Exception {
        switch(storageType) {
            case TiHuan:
                return f1JdbcService.update(jdbcDatas, beforeData, afterData);
            case UpSet:
                return f2JdbcService.update(jdbcDatas, beforeData, afterData);
            case LiShiLaLian:
                return f3JdbcService.update(jdbcDatas, beforeData, afterData);
            default:
                throw new Exception("暂不支持该类型操作");
        }
    }

    public static int delete(StorageType storageType, List<JDBCData> jdbcDatas, Map<String, Object> beforeData) throws Exception {
        switch(storageType) {
            case TiHuan:
                return f1JdbcService.delete(jdbcDatas, beforeData);
            case UpSet:
                return f2JdbcService.delete(jdbcDatas, beforeData);
            case LiShiLaLian:
                return f3JdbcService.delete(jdbcDatas, beforeData);
            default:
                throw new Exception("暂不支持该类型操作");
        }
    }

    public static int reade(StorageType storageType, List<JDBCData> jdbcDatas, Map<String, Object> after) throws Exception {
        return insert(storageType, jdbcDatas, after);
    }

    public static void initTable(List<JDBCData> jdbcDatas, StorageType storageType) {
        for (JDBCData jdbc : jdbcDatas) {
            initTable(storageType, jdbc);
        }
    }

    public static void initTable(StorageType storageType, JDBCData jdbcData) {
        switch(storageType) {
            case TiHuan:
                f1JdbcService.initTable(jdbcData);
                break;
            case UpSet:
                f2JdbcService.initTable(jdbcData);
                break;
            case LiShiLaLian:
                f3JdbcService.initTable(jdbcData);
                break;
            default:
                throw new RuntimeException("暂不支持该类型操作：" + storageType.getValue());
        }
    }

    public static DatabaseWrapper getDb(JDBCBean jdbc) {
        DatabaseWrapper db = ConnectionTool.getDBWrapper(jdbc);
        return db;
    }
}
