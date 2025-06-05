package hyren.serv6.hadoop.commons.hadoop_helper;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.key.HashChoreWoker;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.hadoop.commons.loginAuth.LoginAuthFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HBaseOperator implements Closeable {

    private static String configPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

    private static String platformType = HdfsOperator.PlatformType.normal.name();

    private static String prncipalName = "admin@HADOOP.COM";

    private static String hadoopUserName = System.getProperty("HADOOP_USER_NAME", "hyshf");

    public Configuration conf;

    public Connection connection;

    public Admin admin;

    public HBaseOperator() {
        this(configPath);
    }

    public HBaseOperator(String confDir) {
        this(confDir, platformType);
    }

    public HBaseOperator(String confDir, String platform) {
        this(confDir, platform, prncipalName);
    }

    public HBaseOperator(String confDir, String platform, String prncipal_name) {
        this(confDir, platform, prncipal_name, hadoopUserName);
    }

    public HBaseOperator(String confDir, String platform, String prncipal_name, String hadoop_user_name) {
        if (StringUtil.isNotBlank(confDir)) {
            configPath = confDir.endsWith(File.separator) ? confDir : confDir + File.separator;
        }
        if (StringUtil.isNotBlank(platform)) {
            platformType = platform;
        }
        if (StringUtil.isNotBlank(prncipal_name)) {
            prncipalName = prncipal_name;
        }
        if (StringUtil.isNotBlank(hadoop_user_name)) {
            hadoopUserName = hadoop_user_name;
        }
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);
        conf = LoginAuthFactory.getInstance(platformType, configPath).login(prncipalName);
        try {
            this.connection = ConnectionFactory.createConnection(conf);
            this.admin = connection.getAdmin();
        } catch (IOException e) {
            throw new AppSystemException("创建 HBaseOperator 对象失败! " + e);
        }
    }

    public HBaseOperator(Configuration conf, String keytabPath, String krb5ConfPath) {
        this(conf, keytabPath, krb5ConfPath, prncipalName);
    }

    public HBaseOperator(Configuration conf, String keytabPath, String krb5ConfPath, String prncipal_name) {
        this(conf, platformType, keytabPath, krb5ConfPath, prncipal_name);
    }

    public HBaseOperator(Configuration conf, String platform, String keytabPath, String krb5ConfPath, String prncipal_name) {
        this(conf, platform, keytabPath, krb5ConfPath, prncipal_name, hadoopUserName);
    }

    public HBaseOperator(Configuration conf, String platform, String keytabPath, String krb5ConfPath, String prncipal_name, String hadoop_user_name) {
        if (StringUtil.isNotBlank(platformType)) {
            platformType = platform;
        }
        if (StringUtil.isBlank(keytabPath)) {
            throw new AppSystemException("input keytabPath is null.");
        }
        if (StringUtil.isBlank(krb5ConfPath)) {
            throw new AppSystemException("input krb5ConfPath is null.");
        }
        if (StringUtil.isNotBlank(prncipal_name)) {
            prncipalName = prncipal_name;
        }
        if (StringUtil.isNotBlank(hadoop_user_name)) {
            hadoopUserName = hadoop_user_name;
        }
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);
        conf = LoginAuthFactory.getInstance(platformType, conf).login(prncipalName, keytabPath, krb5ConfPath);
        try {
            this.connection = ConnectionFactory.createConnection(conf);
            this.admin = connection.getAdmin();
        } catch (IOException e) {
            throw new AppSystemException("创建 HBaseOperator 对象失败! " + e);
        }
    }

    public HBaseOperator(long dsl_id, DatabaseWrapper db) {
        this(ProcessingData.getLayerBean(dsl_id, db));
    }

    public HBaseOperator(LayerBean layerBean) {
        this(FileNameUtils.normalize(Constant.STORECONFIGPATH + layerBean.getDsl_name() + File.separator, true), layerBean.getLayerAttr().get(StorageTypeKey.platform), layerBean.getLayerAttr().get(StorageTypeKey.prncipal_name), layerBean.getLayerAttr().get(StorageTypeKey.hadoop_user_name));
    }

    public Table getTable(String tableName) {
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new AppSystemException("根据表名: " + tableName + "获取Table对象失败! e: " + e);
        }
        return table;
    }

    public Table getTable(String name_space, String tableName) {
        Table table;
        try {
            table = connection.getTable(TableName.valueOf(name_space, tableName));
        } catch (IOException e) {
            throw new AppSystemException("根据表名: " + name_space + TableName.NAMESPACE_DELIM + tableName + "获取Table对象失败! e: " + e);
        }
        return table;
    }

    public Table getTable(TableName tableName) throws IOException {
        return connection.getTable(tableName);
    }

    public Admin getAdmin() {
        return admin;
    }

    public void flush(TableName tableName) {
        try {
            admin.flush(tableName);
        } catch (IOException e) {
            throw new AppSystemException("刷新表,同步操作失败!" + e);
        }
    }

    public void truncateTable(String tableName, boolean preserveSplits) {
        try {
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.truncateTable(TableName.valueOf(tableName), preserveSplits);
        } catch (IOException e) {
            throw new AppSystemException("Hbase truncate 表失败!" + e);
        }
    }

    public void createNamespace(String namespace) {
        try {
            NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nd);
        } catch (NamespaceExistException e) {
            log.info("命名空间: " + namespace + " ,已经存在!");
        } catch (IOException ioe) {
            throw new AppSystemException("创建HBase命名空间失败!" + namespace);
        }
    }

    public void dropNamespace(String namespace, boolean force) {
        try {
            if (force) {
                TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
                for (TableName name : tableNames) {
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }
            }
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            throw new AppSystemException("Hbase 删除命名空间失败! 是否强制删除: " + force + " ,异常信息: " + e);
        }
    }

    public boolean existsTable(String tableName) {
        return existsTable(TableName.valueOf(tableName));
    }

    public boolean existsTable(TableName tableName) {
        try {
            return admin.tableExists(tableName);
        } catch (IOException e) {
            throw new AppSystemException("[判断表名是否存在失败! " + e);
        }
    }

    public void createTableWithPartitions(String tableName, String partitions, String... colfams) {
        if (StringUtil.isNumeric(partitions)) {
            createTableHashedPartitions(tableName, Integer.parseInt(partitions), colfams);
        } else {
            createTableCustomPartitions(tableName, partitions, colfams);
        }
    }

    public void createTableHashedPartitions(String tableName, int partitions, String... colfams) {
        if (partitions < 2 || partitions > 2000) {
            throw new IllegalArgumentException("Partitions count must be a nature number which " + "between 2 and 2000 but got an unexpected number: " + partitions);
        }
        HashChoreWoker worker = new HashChoreWoker(1000000, partitions);
        byte[][] splitKeys = worker.calcSplitKeys();
        createTable(tableName, splitKeys, colfams);
        log.info("Created table {} with {} hashed partitions.", tableName, partitions);
    }

    public void createTableCustomPartitions(String tableName, String partitions, String... colfams) {
        HashChoreWoker worker = new HashChoreWoker(partitions);
        byte[][] splitKeys = worker.coustomSplitKeys();
        createTable(tableName, splitKeys, colfams);
        log.info("Created table {} with customized splitKeys: {}.", tableName, partitions);
    }

    public void createTableWithNamespace(String namespace, String tableName, String... colfams) {
        createTable(TableName.valueOf(namespace + TableName.NAMESPACE_DELIM + tableName), 1, null, true, colfams);
    }

    public void createSimpleTable(String tableName) {
        createSimpleTable(TableName.valueOf(tableName));
    }

    public void createSimpleTable(TableName tableName) {
        createTable(tableName, 1, null, true, Bytes.toString(Constant.HBASE_COLUMN_FAMILY));
    }

    public void createTable(String tableName, String... colfams) {
        createTable(TableName.valueOf(tableName), colfams);
    }

    public void createTable(TableName tableName, String... colfams) {
        createTable(tableName, 1, null, true, colfams);
    }

    public void createTable(String tableName, int maxVersions, String... colfams) {
        createTable(TableName.valueOf(tableName), maxVersions, colfams);
    }

    public void createTable(TableName tableName, int maxVersions, String... colfams) {
        createTable(tableName, maxVersions, null, true, colfams);
    }

    public void createTable(String tableName, byte[][] splitKeys, String... colfams) {
        createTable(TableName.valueOf(tableName), splitKeys, colfams);
    }

    public void createTable(TableName tableName, byte[][] splitKeys, String... colfams) {
        createTable(tableName, 1, splitKeys, true, colfams);
    }

    public void createTable(String tableName, byte[][] splitKeys, boolean compress, String... colfams) {
        createTable(TableName.valueOf(tableName), splitKeys, compress, colfams);
    }

    public void createTable(TableName tableName, byte[][] splitKeys, boolean compress, String... colfams) {
        createTable(tableName, 1, splitKeys, compress, colfams);
    }

    public void createTable(String tableName, int maxVersions, byte[][] splitKeys, boolean compress, String... colfams) {
        createTable(TableName.valueOf(tableName), maxVersions, splitKeys, compress, colfams);
    }

    public void createTable(TableName tableName, int maxVersions, byte[][] splitKeys, boolean compress, String... colfams) {
        createTable(tableName, maxVersions, splitKeys, compress, 0, colfams);
    }

    public void createTable(String tableName, int maxVersions, byte[][] splitKeys, boolean compress, int scope, String... colfams) {
        createTable(TableName.valueOf(tableName), maxVersions, splitKeys, compress, scope, colfams);
    }

    public void createTable(TableName tableName, int maxVersions, byte[][] splitKeys, boolean compress, int scope, String... colfams) {
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
        for (String cf : colfams) {
            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            cfdb.setMaxVersions(maxVersions);
            if (compress) {
                cfdb.setCompressionType(Algorithm.SNAPPY);
            }
            cfdb.setScope(scope);
            tdb.setColumnFamily(cfdb.build());
        }
        TableDescriptor tableDescriptor = tdb.build();
        try {
            if (splitKeys != null) {
                admin.createTable(tableDescriptor, splitKeys);
            } else {
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            throw new AppSystemException("创建 HBase 表: " + tableName + " ,失败! " + e);
        }
    }

    public void disableTable(String tableName) {
        disableTable(TableName.valueOf(tableName));
    }

    public void disableTable(String namespace, String tableName) {
        disableTable(TableName.valueOf(namespace + TableName.NAMESPACE_DELIM + tableName));
    }

    public void disableTable(TableName tableName) {
        try {
            admin.disableTable(tableName);
        } catch (IOException e) {
            throw new AppSystemException("禁用表: " + tableName + " ,失败! " + e);
        }
    }

    public void enableTable(String table) {
        enableTable(TableName.valueOf(table));
    }

    public void enableTable(String namespace, String tableName) {
        enableTable(TableName.valueOf(namespace + TableName.NAMESPACE_DELIM + tableName));
    }

    public void enableTable(TableName tableName) {
        try {
            admin.enableTable(tableName);
        } catch (IOException e) {
            throw new AppSystemException("启用表: " + tableName + " ,失败!" + e);
        }
    }

    public void dropTable(String tableName) {
        dropTable(TableName.valueOf(tableName));
    }

    public void dropTable(String namespace, String tableName) {
        dropTable(TableName.valueOf(namespace + TableName.NAMESPACE_DELIM + tableName));
    }

    public void dropTable(TableName tableName) {
        try {
            if (existsTable(tableName)) {
                if (admin.isTableEnabled(tableName))
                    disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (IOException e) {
            throw new AppSystemException("删除表: " + tableName + " ,失败!" + e);
        }
    }

    public void renameTable(String source_table, String target_table) {
        disableTable(source_table);
        createSnapshot(source_table + "_hy_snapshot", source_table);
        cloneSnapshot(source_table + "_hy_snapshot", target_table);
        deleteSnapshot(source_table + "_hy_snapshot");
        dropTable(source_table);
    }

    public void renameTable(TableName source_table, TableName target_table) {
        disableTable(source_table);
        String snapshotName = source_table.getNamespaceAsString() + "_" + source_table.getQualifierAsString() + "_hy_snapshot";
        createSnapshot(snapshotName, source_table);
        cloneSnapshot(snapshotName, target_table);
        deleteSnapshot(snapshotName + "_hy_snapshot");
        dropTable(source_table);
    }

    public void cloneTable(String source_table, String target_table) {
        disableTable(source_table);
        createSnapshot(source_table + "_hy_snapshot", source_table);
        cloneSnapshot(source_table + "_hy_snapshot", target_table);
        deleteSnapshot(source_table + "_hy_snapshot");
        enableTable(source_table);
    }

    public void cloneTable(TableName source_table, TableName target_table) {
        disableTable(source_table);
        String snapshotName = source_table.getNamespaceAsString() + "_" + source_table.getQualifierAsString() + "_hy_snapshot";
        createSnapshot(snapshotName, source_table);
        cloneSnapshot(snapshotName, target_table);
        deleteSnapshot(snapshotName);
        enableTable(source_table);
    }

    public void put(String tableName, String row, String fam, String qual, String val) {
        put(TableName.valueOf(tableName), row, fam, qual, val);
    }

    public void put(TableName tableName, String row, String fam, String qual, String val) {
        try {
            Table table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new AppSystemException("Put 表:" + tableName + ",数据失败!" + e);
        }
    }

    public void put(String tableName, String row, String fam, String qual, long ts, String val) {
        put(TableName.valueOf(tableName), row, fam, qual, ts, val);
    }

    public void put(TableName tableName, String row, String fam, String qual, long ts, String val) {
        try {
            Table table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), ts, Bytes.toBytes(val));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new AppSystemException("Put 表:" + tableName + ",数据失败!" + e);
        }
    }

    public void put(String tableName, String[] rows, String[] fams, String[] quals, long[] ts, String[] vals) {
        put(TableName.valueOf(tableName), rows, fams, quals, ts, vals);
    }

    public void put(TableName tableName, String[] rows, String[] fams, String[] quals, long[] ts, String[] vals) {
        try {
            Table tbl = connection.getTable(tableName);
            for (String row : rows) {
                Put put = new Put(Bytes.toBytes(row));
                for (String fam : fams) {
                    int v = 0;
                    for (String qual : quals) {
                        String val = vals[v < vals.length ? v : vals.length - 1];
                        long t = ts[v < ts.length ? v : ts.length - 1];
                        log.debug("Adding: " + row + " " + fam + " " + qual + " " + t + " " + val);
                        put.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), t, Bytes.toBytes(val));
                        v++;
                    }
                }
                tbl.put(put);
            }
            tbl.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new AppSystemException("Put 表:" + tableName + ",数据失败!" + e);
        }
    }

    @Deprecated
    public void dump(String tableName) {
        dump(TableName.valueOf(tableName));
    }

    @Deprecated
    public void dump(TableName tableName) {
        try (Table t = connection.getTable(tableName);
            ResultScanner scanner = t.getScanner(new Scan())) {
            for (Result scan_result : scanner) {
                dumpResult(scan_result);
            }
        } catch (IOException e) {
            throw new AppSystemException("dump 表:" + tableName + ",数据失败! 这里只是进行debug输出,未做数据处理" + e);
        }
    }

    @Deprecated
    public void dumpResult(Result scan_result) {
        for (Cell cell : scan_result.rawCells()) {
            log.debug("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
    }

    @Deprecated
    public void dump(String tableName, String[] rows, String[] fams, String[] quals) {
        dump(TableName.valueOf(tableName), rows, fams, quals);
    }

    @Deprecated
    public void dump(TableName tableName, String[] rows, String[] fams, String[] quals) {
        try {
            Table table = connection.getTable(tableName);
            List<Get> gets = new ArrayList<>();
            for (String row : rows) {
                Get get = new Get(Bytes.toBytes(row));
                get.setMaxVersions();
                if (fams != null) {
                    for (String fam : fams) {
                        for (String qual : quals) {
                            get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
                        }
                    }
                }
                gets.add(get);
            }
            Result[] results = table.get(gets);
            for (Result result : results) {
                for (Cell cell : result.rawCells()) {
                    log.debug("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }
            table.close();
        } catch (IOException e) {
            throw new AppSystemException("dump 表:" + tableName + ",数据失败! 这里只是进行debug输出,未做数据处理" + e);
        }
    }

    public List<SnapshotDescription> getSnapshots() {
        try {
            return admin.listSnapshots();
        } catch (IOException e) {
            log.error("获取快照列表失败! ", e);
            throw new AppSystemException("获取快照列表失败! " + e);
        }
    }

    public void createSnapshot(String snapshotName, String tableName) {
        createSnapshot(snapshotName, TableName.valueOf(tableName));
    }

    public void createSnapshot(String snapshotName, TableName tableName) {
        try {
            admin.snapshot(snapshotName, tableName);
            log.info("create " + snapshotName + " successful!");
        } catch (IOException e) {
            e.printStackTrace();
            throw new AppSystemException("创建表:" + tableName + " ,镜像: " + snapshotName + " ,失败!" + e);
        }
    }

    public void cloneSnapshot(String snapshotName, String tableName) {
        cloneSnapshot(snapshotName, TableName.valueOf(tableName));
    }

    public void cloneSnapshot(String snapshotName, TableName tableName) {
        try {
            admin.cloneSnapshot(snapshotName, tableName);
            log.info("clone " + snapshotName + " successful!");
        } catch (IOException e) {
            throw new AppSystemException("克隆表: " + tableName + " ,镜像: " + snapshotName + "失败!" + e);
        }
    }

    public void restoreSnapshot(String snapshotName, String tableName) {
        try {
            disableTable(tableName);
            admin.restoreSnapshot(snapshotName);
            enableTable(tableName);
            log.info("restore table " + tableName + " successful via " + snapshotName);
        } catch (IOException e) {
            e.printStackTrace();
            throw new AppSystemException("恢复表:" + tableName + " ,失败!" + tableName);
        }
    }

    public void deleteSnapshot(String snapshotName) {
        try {
            admin.deleteSnapshot(snapshotName);
            log.info("Delete snapshot " + snapshotName + "successful!");
        } catch (IOException e) {
            throw new AppSystemException("删除快照: " + snapshotName + " ,失败!" + e);
        }
    }

    public List<String> listNamespaceName() {
        try {
            List<String> list = new ArrayList<>();
            NamespaceDescriptor[] listNamespaceDescriptors = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor namespaceDescriptor : listNamespaceDescriptors) {
                String nameSpaceName = namespaceDescriptor.getName();
                list.add(nameSpaceName);
            }
            return list;
        } catch (IOException e) {
            throw new AppSystemException("获取命名空间列表失败!" + e);
        }
    }

    public Map<String, List<String>> listNameSpaceWithTableNames() {
        try {
            Map<String, List<String>> map = new HashMap<>();
            List<String> nameSpaces = listNamespaceName();
            for (String nameSpace : nameSpaces) {
                List<String> tableNames = new ArrayList<>();
                TableName[] listTableNamesByNamespace = admin.listTableNamesByNamespace(nameSpace);
                for (TableName tableName : listTableNamesByNamespace) {
                    String tableNameString = tableName.getNameAsString();
                    List<String> split = StringUtil.split(tableNameString, String.valueOf(TableName.NAMESPACE_DELIM));
                    if (split.size() > 1) {
                        tableNames.add(split.get(1));
                    } else {
                        tableNames.add(tableName.getNameAsString());
                    }
                }
                map.put(nameSpace, tableNames);
            }
            return map;
        } catch (IOException e) {
            throw new AppSystemException("获取命名空间及表列表失败!" + e);
        }
    }

    @Override
    public void close() throws IOException {
        if (admin != null)
            admin.close();
        if (connection != null)
            connection.close();
    }
}
