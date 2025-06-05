package hyren.serv6.hadoop.commons.imp;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.i.IHbase;
import hyren.serv6.commons.utils.agent.Increasement;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.PropertyParaUtil;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.hadoop.agent.biz.bulkload.*;
import hyren.serv6.hadoop.commons.hadoop_helper.HBaseOperator;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.hadoop.increasement.HBaseIncreasement;
import hyren.serv6.hadoop.increasement.impl.HBaseIncreasementByHive;
import hyren.serv6.hadoop.increasement.impl.HBaseIncreasementByPhoenix;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HbaseImp implements IHbase {

    @Override
    public void backupToDayTable(String todayTableName) {
        backupTable(todayTableName);
    }

    @Override
    public void backupPastTable(Long storage_time, String storageTableName, String etlDate, String storageDate) {
        try (HBaseOperator hBaseOperator = new HBaseOperator()) {
            String underline1bTableName = TableNameUtil.getUnderline1bTableName(storageTableName);
            if (etlDate.equals(storageDate) || storage_time == 1) {
                if (hBaseOperator.existsTable(underline1bTableName)) {
                    hBaseOperator.dropTable(underline1bTableName);
                }
            } else {
                for (long i = storage_time; i > 1; i--) {
                    if (hBaseOperator.existsTable(TableNameUtil.getSpliceTableName(storageTableName, i))) {
                        if (i == storage_time) {
                            hBaseOperator.dropTable(TableNameUtil.getSpliceTableName(storageTableName, i));
                        } else {
                            if (hBaseOperator.existsTable(TableNameUtil.getSpliceTableName(storageTableName, i + 1))) {
                                hBaseOperator.dropTable(TableNameUtil.getSpliceTableName(storageTableName, i + 1));
                            }
                            hBaseOperator.renameTable(TableNameUtil.getSpliceTableName(storageTableName, i), TableNameUtil.getSpliceTableName(storageTableName, i + 1));
                        }
                    }
                }
                if (hBaseOperator.existsTable(underline1bTableName)) {
                    if (hBaseOperator.existsTable(TableNameUtil.getSpliceTableName(storageTableName, 2))) {
                        hBaseOperator.dropTable(TableNameUtil.getSpliceTableName(storageTableName, 2));
                    }
                    hBaseOperator.renameTable(underline1bTableName, TableNameUtil.getSpliceTableName(storageTableName, 2));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void recoverBackupToDayTable(String todayTableName) {
        backupTable(todayTableName);
    }

    private static void backupTable(String tableName) {
        try (HBaseOperator hBaseOperator = new HBaseOperator()) {
            String backupTableNameSuffixB = TableNameUtil.getBackupTableNameSuffixB(tableName);
            if (hBaseOperator.existsTable(backupTableNameSuffixB)) {
                if (hBaseOperator.existsTable(tableName)) {
                    hBaseOperator.dropTable(tableName);
                }
                hBaseOperator.renameTable(backupTableNameSuffixB, tableName);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final byte[] FAP = "file_avro_path".getBytes();

    private static final byte[] FAB = "file_avro_block".getBytes();

    private static final byte[] CF = Constant.HBASE_COLUMN_FAMILY;

    @Override
    public void saveInHbase(List<String[]> hbaseList, String sysDate) {
        log.info("Start to saveInHbase...");
        if (hbaseList == null) {
            return;
        }
        List<Put> putList = new ArrayList<>();
        for (String[] str : hbaseList) {
            Put put = new Put(str[0].getBytes());
            if (str.length == 1) {
                put.addColumn(CF, FileCollectParamBean.E_DATE, sysDate.getBytes());
            } else if (str.length == 3) {
                put.addColumn(CF, FAP, str[1].getBytes());
                put.addColumn(CF, FAB, str[2].getBytes());
            } else if (str.length == 4) {
                put.addColumn(CF, FileCollectParamBean.S_DATE, sysDate.getBytes());
                put.addColumn(CF, FileCollectParamBean.E_DATE, FileCollectParamBean.MAXDATE);
                put.addColumn(CF, FAP, str[1].getBytes());
                put.addColumn(CF, FAB, str[2].getBytes());
            } else {
                throw new AppSystemException("saving Hbase fails because strs.length is wrong ...");
            }
            putList.add(put);
        }
        String configPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        String platform = PropertyParaUtil.getString("platform", HdfsOperator.PlatformType.normal.toString());
        String prncipal_name = PropertyParaUtil.getString("principle.name", "admin@HADOOP.COM");
        String hadoop_user_name = PropertyParaUtil.getString("HADOOP_USER_NAME", "hyshf");
        try (HBaseOperator hBaseOperator = new HBaseOperator(configPath, platform, prncipal_name, hadoop_user_name)) {
            if (!hBaseOperator.existsTable(FileCollectParamBean.FILE_HBASE_NAME)) {
                HBaseIncreasement.createDefaultPrePartTable(hBaseOperator, FileCollectParamBean.FILE_HBASE_NAME, true);
            }
            Table table = hBaseOperator.getTable(Bytes.toString(FileCollectParamBean.FILE_HBASE));
            table.put(putList);
        } catch (Exception e) {
            log.error("Failed to putInHbase", e);
            throw new AppSystemException("Failed to putInHbase..." + e.getMessage());
        }
    }

    @Override
    public Increasement getHBaseIncreasement(TableBean tableBean, String hbase_name, String etlDate, DataStoreConfBean dataStoreConf) {
        HBaseIncreasement hbaseIncreasement;
        Map<String, String> data_store_connect_attr = dataStoreConf.getData_store_connect_attr();
        String dsl_name = dataStoreConf.getDsl_name();
        if ("hive".equalsIgnoreCase(data_store_connect_attr.get(StorageTypeKey.increment_engine))) {
            if (StringUtil.isBlank(data_store_connect_attr.get(StorageTypeKey.jdbc_url))) {
                hbaseIncreasement = new HBaseIncreasementByHive(tableBean, hbase_name, etlDate, dsl_name, data_store_connect_attr.get(StorageTypeKey.hadoop_user_name), data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), dataStoreConf.getDsl_id(), null);
            } else {
                dataStoreConf.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                DatabaseWrapper db = ConnectionTool.getDBWrapper(dataStoreConf.getData_store_connect_attr());
                hbaseIncreasement = new HBaseIncreasementByHive(tableBean, hbase_name, etlDate, dsl_name, data_store_connect_attr.get(StorageTypeKey.hadoop_user_name), data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), dataStoreConf.getDsl_id(), db);
            }
        } else if ("phoenix".equalsIgnoreCase(data_store_connect_attr.get(StorageTypeKey.increment_engine))) {
            if (StringUtil.isBlank(data_store_connect_attr.get(StorageTypeKey.jdbc_url))) {
                hbaseIncreasement = new HBaseIncreasementByPhoenix(tableBean, hbase_name, etlDate, dsl_name, data_store_connect_attr.get(StorageTypeKey.hadoop_user_name), data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), dataStoreConf.getDsl_id(), null);
            } else {
                DatabaseWrapper db = ConnectionTool.getDBWrapper(dataStoreConf.getData_store_connect_attr());
                hbaseIncreasement = new HBaseIncreasementByPhoenix(tableBean, hbase_name, etlDate, dsl_name, data_store_connect_attr.get(StorageTypeKey.hadoop_user_name), data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), dataStoreConf.getDsl_id(), db);
            }
        } else {
            throw new AppSystemException("HBase算增量不支持其他计算引擎");
        }
        return hbaseIncreasement;
    }

    @Override
    public void loadDataToHbase(String todayTableName, String hdfsFilePath, TableBean tableBean, CollectTableBean collectTableBean, String isMd5, StringBuilder rowKeyIndex, DataStoreConfBean dataStoreConfBean) {
        Map<String, String> storeLayerAttr = dataStoreConfBean.getData_store_connect_attr();
        String configPath = FileNameUtils.normalize(Constant.STORECONFIGPATH + dataStoreConfBean.getDsl_name() + File.separator, true);
        try (HBaseOperator hBaseOperator = new HBaseOperator(configPath, storeLayerAttr.get(StorageTypeKey.platform), storeLayerAttr.get(StorageTypeKey.prncipal_name), storeLayerAttr.get(StorageTypeKey.hadoop_user_name))) {
            backupToDayTable(todayTableName);
            HBaseIncreasement.createDefaultPrePartTable(hBaseOperator, todayTableName, false);
            int run;
            String etlDate = collectTableBean.getEtlDate();
            String file_format = tableBean.getFile_format();
            String columnMetaInfo = tableBean.getColumnMetaInfo();
            String[] args = { todayTableName, hdfsFilePath, columnMetaInfo, rowKeyIndex.toString(), configPath, etlDate, isMd5, storeLayerAttr.get(StorageTypeKey.hadoop_user_name), storeLayerAttr.get(StorageTypeKey.platform), storeLayerAttr.get(StorageTypeKey.prncipal_name), tableBean.getIs_header() };
            log.info("========file_format:{}=========", FileFormat.ofValueByCode(file_format));
            if (FileFormat.SEQUENCEFILE.getCode().equals(file_format)) {
                run = ToolRunner.run(hBaseOperator.conf, new SequeceBulkLoadJob(), args);
            } else if (FileFormat.FeiDingChang.getCode().equals(file_format)) {
                String[] args2 = { todayTableName, hdfsFilePath, columnMetaInfo, rowKeyIndex.toString(), configPath, etlDate, isMd5, storeLayerAttr.get(StorageTypeKey.hadoop_user_name), storeLayerAttr.get(StorageTypeKey.platform), storeLayerAttr.get(StorageTypeKey.prncipal_name), tableBean.getColumn_separator(), tableBean.getIs_header() };
                run = ToolRunner.run(hBaseOperator.conf, new NonFixedBulkLoadJob(), args2);
            } else if (FileFormat.PARQUET.getCode().equals(file_format)) {
                run = ToolRunner.run(hBaseOperator.conf, new ParquetBulkLoadJob(), args);
            } else if (FileFormat.ORC.getCode().equals(file_format)) {
                run = ToolRunner.run(hBaseOperator.conf, new OrcBulkLoadJob(), args);
            } else if (FileFormat.DingChang.getCode().equals(file_format)) {
                String[] args2 = { todayTableName, hdfsFilePath, columnMetaInfo, rowKeyIndex.toString(), configPath, etlDate, isMd5, storeLayerAttr.get(StorageTypeKey.hadoop_user_name), storeLayerAttr.get(StorageTypeKey.platform), storeLayerAttr.get(StorageTypeKey.prncipal_name), DataBaseCode.ofValueByCode(tableBean.getFile_code()), tableBean.getColLengthInfo(), tableBean.getIs_header() };
                run = ToolRunner.run(hBaseOperator.conf, new FixedBulkLoadJob(), args2);
            } else if (FileFormat.CSV.getCode().equals(file_format)) {
                run = ToolRunner.run(hBaseOperator.conf, new CsvBulkLoadJob(), args);
            } else {
                throw new AppSystemException("暂不支持定长或者其他类型直接加载到hive表");
            }
            if (run != 0) {
                throw new AppSystemException("数据加载table hbase 失败 " + todayTableName);
            }
            backupPastTable(collectTableBean.getStorage_time(), collectTableBean.getStorage_table_name(), etlDate, collectTableBean.getStorage_date());
        } catch (Exception e) {
            recoverBackupToDayTable(todayTableName);
            throw new AppSystemException("数据加载table hbase 失败 " + todayTableName, e);
        }
    }

    public void loadObjDataToHBase(String todayTableName, String hdfsFilePath, TableBean tableBean, String isMd5, StringBuilder rowKeyIndex, String etlDate, DataStoreConfBean dataStoreConfBean) {
        Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
        String configPath = FileNameUtils.normalize(Constant.STORECONFIGPATH + dataStoreConfBean.getDsl_name() + File.separator, true);
        try (HBaseOperator hBaseOperator = new HBaseOperator(configPath, data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), data_store_connect_attr.get(StorageTypeKey.hadoop_user_name))) {
            hBaseOperator.dropTable(todayTableName);
            HBaseIncreasement.createDefaultPrePartTable(hBaseOperator, todayTableName, false);
            String file_format = tableBean.getFile_format();
            String columnMetaInfo = tableBean.getColumnMetaInfo();
            String[] args = { todayTableName, hdfsFilePath, columnMetaInfo, rowKeyIndex.toString(), configPath, etlDate, isMd5, data_store_connect_attr.get(StorageTypeKey.hadoop_user_name), data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), tableBean.getIs_header() };
            int run;
            if (FileFormat.SEQUENCEFILE.getCode().equals(file_format)) {
                run = ToolRunner.run(hBaseOperator.conf, new SequeceBulkLoadJob(), args);
            } else if (FileFormat.FeiDingChang.getCode().equals(file_format)) {
                String[] args2 = { todayTableName, hdfsFilePath, columnMetaInfo, rowKeyIndex.toString(), configPath, etlDate, isMd5, data_store_connect_attr.get(StorageTypeKey.hadoop_user_name), data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), tableBean.getColumn_separator(), tableBean.getIs_header() };
                run = ToolRunner.run(hBaseOperator.conf, new NonFixedBulkLoadJob(), args2);
            } else if (FileFormat.PARQUET.getCode().equals(file_format)) {
                run = ToolRunner.run(hBaseOperator.conf, new ParquetBulkLoadJob(), args);
            } else if (FileFormat.ORC.getCode().equals(file_format)) {
                run = ToolRunner.run(hBaseOperator.conf, new OrcBulkLoadJob(), args);
            } else if (FileFormat.DingChang.getCode().equals(file_format)) {
                String[] args2 = { todayTableName, hdfsFilePath, columnMetaInfo, rowKeyIndex.toString(), configPath, etlDate, isMd5, data_store_connect_attr.get(StorageTypeKey.hadoop_user_name), data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), DataBaseCode.ofValueByCode(tableBean.getFile_code()), tableBean.getColLengthInfo(), tableBean.getIs_header() };
                run = ToolRunner.run(hBaseOperator.conf, new FixedBulkLoadJob(), args2);
            } else if (FileFormat.CSV.getCode().equals(file_format)) {
                run = ToolRunner.run(hBaseOperator.conf, new CsvBulkLoadJob(), args);
            } else {
                throw new AppSystemException("暂不支持定长或者其他类型直接加载到hive表");
            }
            if (run != 0) {
                throw new AppSystemException("数据加载table hbase 失败 " + todayTableName);
            }
        } catch (Exception e) {
            throw new AppSystemException("数据加载table hbase 失败 " + todayTableName, e);
        }
    }

    public List<Map<String, Object>> queryByRowkey(String tableName, String rowkey, String versions, String dsl_name, String platform, String prncipal_name, String hadoop_user_name, List<String> colWithCf) throws IOException {
        try (HBaseOperator hBaseOperator = new HBaseOperator(FileNameUtils.normalize(Constant.STORECONFIGPATH + dsl_name + File.separator, true), platform, prncipal_name, hadoop_user_name);
            Table table = hBaseOperator.getTable(tableName)) {
            Get get = new Get(rowkey.getBytes());
            if (StringUtil.isNotBlank(versions)) {
                get.setTimeStamp(Long.parseLong(versions));
            }
            if (!colWithCf.isEmpty()) {
                get.addColumn(colWithCf.get(0).getBytes(), colWithCf.get(1).getBytes());
            }
            Result result = table.get(get);
            List<Cell> cellList = result.listCells();
            if (cellList != null && cellList.size() > 0) {
                List<Map<String, Object>> jsonList = new ArrayList<>();
                for (Cell cell : cellList) {
                    Map<String, Object> map = new HashMap<>();
                    map.put(Bytes.toString(CellUtil.cloneQualifier(cell)).toLowerCase(), Bytes.toString(CellUtil.cloneValue(cell)));
                    jsonList.add(map);
                }
                return jsonList;
            } else {
                return new ArrayList<>();
            }
        }
    }

    public List<Map<String, Object>> queryByRowkey(String tableName, String dsl_name, String platform, String prncipal_name, String hadoop_user_name, String selectColumn, List<String> rowkeyList) throws IOException {
        try (HBaseOperator hBaseOperator = new HBaseOperator(FileNameUtils.normalize(Constant.STORECONFIGPATH + dsl_name + File.separator, true), platform, prncipal_name, hadoop_user_name)) {
            Table table = hBaseOperator.getTable(tableName);
            List<Get> getList = new ArrayList<>();
            for (String rowkey : rowkeyList) {
                log.info("rowkey: " + rowkey);
                Get get = new Get(rowkey.getBytes());
                getList.add(get);
            }
            List<Map<String, Object>> js = new ArrayList<>();
            Result[] results = table.get(getList);
            Map<String, Object> temp;
            List<String> selectList = StringUtil.split(selectColumn.toLowerCase(), Constant.METAINFOSPLIT);
            for (Result hResult : results) {
                if (hResult.isEmpty()) {
                    log.info("rowkey查询结果为空");
                    continue;
                }
                temp = new HashMap<>();
                List<Cell> cs = hResult.listCells();
                for (Cell cell : cs) {
                    String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    if (selectList.contains(column.toLowerCase())) {
                        temp.put(column.toLowerCase(), value);
                    }
                }
                js.add(temp);
            }
            return js;
        }
    }

    public List<String> deleteHBaseData(String tableName, String[] rowKeys) throws IOException {
        try (HBaseOperator hBaseOperator = new HBaseOperator()) {
            if (hBaseOperator.existsTable(tableName)) {
                try (Table table = hBaseOperator.getTable(tableName)) {
                    List<Delete> deleteList = new ArrayList<>();
                    List<String> rowkeyList = new ArrayList<>();
                    for (String rowkey : rowKeys) {
                        Get get = new Get(rowkey.getBytes());
                        if (!table.get(get).isEmpty()) {
                            Delete delete = new Delete(rowkey.getBytes());
                            deleteList.add(delete);
                        } else {
                            rowkeyList.add(rowkey);
                        }
                    }
                    if (rowkeyList.size() == 0) {
                        table.delete(deleteList);
                    }
                    return rowkeyList;
                }
            } else {
                return null;
            }
        }
    }
}
