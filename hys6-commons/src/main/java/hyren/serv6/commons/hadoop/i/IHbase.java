package hyren.serv6.commons.hadoop.i;

import hyren.serv6.commons.utils.agent.Increasement;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IHbase {

    void backupToDayTable(String todayTableName);

    void backupPastTable(Long storage_time, String storageTableName, String etlDate, String storageDate);

    void recoverBackupToDayTable(String todayTableName);

    void saveInHbase(List<String[]> hbaseList, String sysDate);

    Increasement getHBaseIncreasement(TableBean tableBean, String hbase_name, String etlDate, DataStoreConfBean dataStoreConf);

    void loadDataToHbase(String todayTableName, String hdfsFilePath, TableBean tableBean, CollectTableBean collectTableBean, String isMd5, StringBuilder rowKeyIndex, DataStoreConfBean storeLayerAttr);

    void loadObjDataToHBase(String todayTableName, String hdfsFilePath, TableBean tableBean, String isMd5, StringBuilder rowKeyIndex, String etlDate, DataStoreConfBean dataStoreConfBean);

    List<Map<String, Object>> queryByRowkey(String en_table, String rowkey, String versions, String dsl_name, String platform, String prncipal_name, String hadoop_user_name, List<String> colWithCf) throws IOException;

    List<Map<String, Object>> queryByRowkey(String tableName, String dsl_name, String platform, String prncipal_name, String hadoop_user_name, String selectColumn, List<String> rowkeyList) throws IOException;

    List<String> deleteHBaseData(String tableName, String[] rowKeys) throws IOException;
}
