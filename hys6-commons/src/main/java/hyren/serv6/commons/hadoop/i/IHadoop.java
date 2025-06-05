package hyren.serv6.commons.hadoop.i;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.hadoop.algorithms.conf.AlgorithmsConf;
import hyren.serv6.commons.key.HashChoreWoker;
import hyren.serv6.commons.utils.agent.bean.AvroBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

public interface IHadoop {

    void setHiveConf(Map<String, String> dbConfig);

    void createHBaseTable(String name_space, String hbase_table_name, LayerBean layerBean);

    void hbaseDropTable(String tableName, LayerBean layerBean);

    void hbaserenameTable(String old_table_name, String new_table_name, LayerBean layerBean);

    TreeSet<byte[]> coustomByteAddData(int baseRecord, String[] split);

    TreeSet<byte[]> calcByteAddData(int baseRecord, HashChoreWoker.IRowKeyGenerator rkGen);

    byte[] nextId(long currentId, long currentTime, Random random);

    byte[] stringtoByte(String str);

    String byteToString(byte[] bytes);

    void createHiveDecimal(BigDecimal dataResult, List<Object> lineData);

    void copyFileToHDFS(String localPath, String hdfsPath);

    void mkdirHdfs(String fileCollectHdfsPath);

    List<AvroBean> getAvroBeans(String fileCollectHdfsPath, String avroFileAbsolutionPath);

    void execHDFSShell(DataStoreConfBean dataStoreConfBean, String localFilePath, String tableName, String hdfsPath) throws Exception;

    void importDataToDatabase(AlgorithmsConf algorithmsConf, String HyFlag, DatabaseWrapper db);
}
