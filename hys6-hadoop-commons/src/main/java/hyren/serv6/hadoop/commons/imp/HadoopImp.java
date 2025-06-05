package hyren.serv6.hadoop.commons.imp;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.SystemUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.i.IHadoop;
import hyren.serv6.commons.hadoop.algorithms.conf.AlgorithmsConf;
import hyren.serv6.hadoop.commons.algorithms.impl.ImportHyFdData;
import hyren.serv6.hadoop.commons.algorithms.impl.ImportHyUCCData;
import hyren.serv6.hadoop.commons.hadoop_helper.HBaseOperator;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.commons.key.HashChoreWoker;
import hyren.serv6.commons.utils.agent.bean.AvroBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.agent.constant.PropertyParaUtil;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class HadoopImp implements IHadoop {

    @Override
    public void setHiveConf(Map<String, String> dbConfig) {
        String platformType = dbConfig.get(StorageTypeKey.platform);
        if (StringUtil.isNotBlank(platformType) && !platformType.equalsIgnoreCase(HdfsOperator.PlatformType.normal.name())) {
            String hive_site_xml_path = dbConfig.get(StorageTypeKey.hive_site);
            String platform = dbConfig.get(StorageTypeKey.platform);
            String prncipal_name = dbConfig.get(StorageTypeKey.prncipal_name);
            String hadoop_user_name = dbConfig.get(StorageTypeKey.hadoop_user_name);
            if (StringUtil.isNotBlank(hive_site_xml_path)) {
                String configPath = hive_site_xml_path.substring(0, hive_site_xml_path.lastIndexOf("/") + 1);
                new HdfsOperator(configPath, platform, prncipal_name, hadoop_user_name);
            }
        }
    }

    @Override
    public void createHBaseTable(String name_space, String hbase_table_name, LayerBean layerBean) {
        if (name_space.equalsIgnoreCase(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
            throw new BusinessException("不允许在系统命名空间下创建表");
        }
        if (StringUtil.isBlank(name_space)) {
            name_space = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
        }
        try (HBaseOperator hBaseOperator = new HBaseOperator(layerBean)) {
            if (hBaseOperator.existsTable(name_space + ":" + hbase_table_name)) {
                throw new BusinessException("待创建的HBase表已经在存储层中存在! 表名: " + name_space + ":" + hbase_table_name);
            }
            Admin admin = hBaseOperator.getAdmin();
            TableName tableName;
            if (!name_space.equalsIgnoreCase(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR)) {
                try {
                    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name_space).build();
                    admin.createNamespace(namespaceDescriptor);
                } catch (IOException ioe) {
                    throw new BusinessException("创建HBase命名空间失败!" + name_space);
                }
                tableName = TableName.valueOf(name_space, hbase_table_name);
            } else {
                tableName = TableName.valueOf(hbase_table_name);
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            String[] column_familie_s = new String[] { "cf1", "cf2" };
            for (String column_familie : column_familie_s) {
                if (StringUtil.isBlank(column_familie)) {
                    continue;
                }
                HColumnDescriptor col_desc = new HColumnDescriptor(column_familie);
                int max_version = 10;
                col_desc.setMaxVersions(max_version);
                IsFlag is_compress = IsFlag.ofEnumByCode("1");
                if (is_compress == IsFlag.Shi) {
                    col_desc.setCompressionType(Compression.Algorithm.SNAPPY);
                }
                IsFlag is_use_bloom_filter = IsFlag.ofEnumByCode("1");
                if (is_use_bloom_filter == IsFlag.Shi) {
                    String bloom_filter_type = "ROW";
                    BloomType bloomType = BloomType.valueOf(bloom_filter_type);
                    if (bloomType == BloomType.ROW) {
                        col_desc.setBloomFilterType(BloomType.ROW);
                    } else if (bloomType == BloomType.ROWCOL) {
                        col_desc.setBloomFilterType(BloomType.ROWCOL);
                    } else {
                        throw new BusinessException("BloomType类型不合法!" + " {ROW: 根据KeyValue中的row来过滤storefile; ROWCOL: 根据KeyValue中的row+qualifier来过滤storefile}");
                    }
                } else if (is_use_bloom_filter == IsFlag.Fou) {
                    col_desc.setBloomFilterType(BloomType.NONE);
                }
                int block_size = 65536;
                col_desc.setBlocksize(block_size);
                IsFlag is_in_memory = IsFlag.ofEnumByCode("1");
                if (is_in_memory == IsFlag.Shi) {
                    col_desc.setInMemory(Boolean.TRUE);
                } else if (is_in_memory == IsFlag.Fou) {
                    col_desc.setInMemory(Boolean.FALSE);
                } else {
                    throw new BusinessException("列族: " + column_familie + ", 是否加入块缓存的配置不合法! is_in_memory: " + is_in_memory.getCode());
                }
                String data_block_encoding = "NONE";
                DataBlockEncoding dataBlockEncoding = DataBlockEncoding.valueOf(data_block_encoding);
                if (dataBlockEncoding == DataBlockEncoding.NONE || dataBlockEncoding == DataBlockEncoding.PREFIX || dataBlockEncoding == DataBlockEncoding.DIFF || dataBlockEncoding == DataBlockEncoding.FAST_DIFF || dataBlockEncoding == DataBlockEncoding.ROW_INDEX_V1) {
                    col_desc.setDataBlockEncoding(dataBlockEncoding);
                } else {
                    throw new BusinessException("数据块编码方式不合法! data_block_encoding=" + dataBlockEncoding.name());
                }
                hTableDescriptor.addFamily(col_desc);
            }
            String pre_split = "SPLITNUM";
            String pre_parm = "2000000";
            byte[][] splitKeys = null;
            if (StringUtil.isNotBlank(pre_split)) {
                splitKeys = null;
            }
            if (splitKeys != null) {
                admin.createTable(hTableDescriptor, splitKeys);
            } else {
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new BusinessException("执行HBase类型存储层创建表失败! table_name=" + hbase_table_name);
        }
    }

    @Override
    public void hbaseDropTable(String tableName, LayerBean layerBean) {
        try (HBaseOperator hBaseOperator = new HBaseOperator(layerBean)) {
            if (hBaseOperator.existsTable(tableName)) {
                hBaseOperator.dropTable(tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new BusinessException("删除HBase存储类型的数据表失败!");
        }
    }

    @Override
    public void hbaserenameTable(String old_table_name, String new_table_name, LayerBean layerBean) {
        try (HBaseOperator hBaseOperator = new HBaseOperator(layerBean)) {
            if (hBaseOperator.existsTable(old_table_name)) {
                if (hBaseOperator.existsTable(new_table_name)) {
                    throw new BusinessException("重命名HBase表时,修改后的表名已经存在!");
                }
                hBaseOperator.renameTable(old_table_name, new_table_name);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new BusinessException("重名HBase表失败!");
        }
    }

    @Override
    public TreeSet<byte[]> coustomByteAddData(int baseRecord, String[] split) {
        TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < baseRecord; i++) {
            rows.add(Bytes.toBytes(split[i]));
        }
        return rows;
    }

    @Override
    public TreeSet<byte[]> calcByteAddData(int baseRecord, HashChoreWoker.IRowKeyGenerator rkGen) {
        TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < baseRecord; i++) {
            rows.add(rkGen.nextId());
        }
        return rows;
    }

    @Override
    public byte[] nextId(long currentId, long currentTime, Random random) {
        try {
            currentTime += random.nextInt(1000);
            byte[] lowT = Bytes.copy(Bytes.toBytes(currentTime), 4, 4);
            byte[] lowU = Bytes.copy(Bytes.toBytes(currentId), 4, 4);
            byte[] currId = Bytes.toBytes(String.valueOf(currentId));
            return Bytes.add(MD5Hash.getMD5AsHex(Bytes.add(lowU, lowT)).substring(0, 8).getBytes(), currId);
        } finally {
            currentId++;
        }
    }

    @Override
    public byte[] stringtoByte(String str) {
        return Bytes.toBytes(str);
    }

    @Override
    public String byteToString(byte[] bytes) {
        return Bytes.toString(bytes);
    }

    @Override
    public void createHiveDecimal(BigDecimal bigDecimal, List<Object> lineData) {
        HiveDecimal create = HiveDecimal.create(bigDecimal);
        lineData.add(create);
    }

    @Override
    public void copyFileToHDFS(String localPath, String hdfsPath) {
        try (HdfsOperator operator = new HdfsOperator(System.getProperty("user.dir") + File.separator + "conf" + File.separator, PropertyParaUtil.getString("platform", HdfsOperator.PlatformType.normal.toString()), PropertyParaUtil.getString("principle.name", "admin@HADOOP.COM"), PropertyParaUtil.getString("HADOOP_USER_NAME", "hyshf"))) {
            operator.upLoad(localPath, hdfsPath, true);
        } catch (Exception e) {
            if (e instanceof BusinessException) {
                throw (BusinessException) e;
            } else {
                throw new AppSystemException(e);
            }
        }
    }

    @Override
    public void mkdirHdfs(String fileCollectHdfsPath) {
        try (HdfsOperator operator = new HdfsOperator(System.getProperty("user.dir") + File.separator + "conf" + File.separator, PropertyParaUtil.getString("platform", HdfsOperator.PlatformType.normal.toString()), PropertyParaUtil.getString("principle.name", "admin@HADOOP.COM"), PropertyParaUtil.getString("HADOOP_USER_NAME", "hyshf"))) {
            Path path = new Path(fileCollectHdfsPath);
            if (!operator.exists(path)) {
                if (!operator.mkdir(path)) {
                    log.info("创建 " + path + " 失败！！！");
                }
            }
        } catch (Exception e) {
            log.error("检查当前环境是否配置集群客户端", e);
            throw new AppSystemException("初始化文件采集数据加载类失败");
        }
    }

    @SneakyThrows
    @Override
    public List<AvroBean> getAvroBeans(String fileCollectHdfsPath, String avroFileAbsolutionPath) {
        Path avroPath = new Path(fileCollectHdfsPath + FileNameUtils.getName(avroFileAbsolutionPath));
        log.info("处理  -->" + avroPath.getName());
        if (!JobConstant.FILE_COLLECTION_IS_WRITE_HADOOP) {
            avroPath = new Path(avroFileAbsolutionPath);
        }
        return getAvroBeans(avroPath);
    }

    private static List<AvroBean> getAvroBeans(Path avroFilePath) throws IOException {
        List<AvroBean> avroBeans = new ArrayList<>();
        int i = 0;
        InputStream is = null;
        DataFileStream<Object> reader = null;
        try (HdfsOperator hdfsOperator = new HdfsOperator(System.getProperty("user.dir") + File.separator + "conf" + File.separator, PropertyParaUtil.getString("platform", HdfsOperator.PlatformType.normal.toString()), PropertyParaUtil.getString("principle.name", "admin@HADOOP.COM"), PropertyParaUtil.getString("HADOOP_USER_NAME", "hyshf"))) {
            if (JobConstant.FILE_COLLECTION_IS_WRITE_HADOOP) {
                is = hdfsOperator.fileSystem.open(avroFilePath);
            } else {
                is = Files.newInputStream(Paths.get(avroFilePath.toString()));
            }
            reader = new DataFileStream<>(is, new GenericDatumReader<>());
            AvroBean avroBean;
            for (Object obj : reader) {
                GenericRecord r = (GenericRecord) obj;
                avroBean = new AvroBean();
                if (i % 1000 == 0) {
                    log.info("[info]读取第" + i + "个文件");
                }
                avroBean.setUuid(r.get("uuid").toString());
                avroBean.setFile_name(r.get("file_name").toString());
                avroBean.setFile_scr_path(r.get("file_scr_path").toString());
                avroBean.setFile_size(r.get("file_size").toString());
                avroBean.setFile_time(r.get("file_time").toString());
                avroBean.setFile_summary(r.get("file_summary").toString().replace(" ", ""));
                avroBean.setFile_text(r.get("file_text").toString());
                avroBean.setFile_md5(r.get("file_md5").toString());
                avroBean.setFile_avro_path(r.get("file_avro_path").toString());
                avroBean.setFile_avro_block(r.get("file_avro_block").toString());
                avroBean.setIs_big_file(r.get("is_big_file").toString());
                avroBean.setIs_increasement(r.get("is_increasement").toString());
                avroBean.setIs_cache(r.get("is_cache").toString());
                avroBeans.add(avroBean);
                i++;
            }
            log.info("读取 avro 文件完毕：一共读取了 " + i + " 个文件");
        } catch (Exception e) {
            log.error("Failed to getAvroBeans...", e);
            throw e;
        } finally {
            try {
                if (null != reader)
                    reader.close();
                if (null != is)
                    is.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        return avroBeans;
    }

    @Override
    public void execHDFSShell(DataStoreConfBean dataStoreConfBean, String localFilePath, String tableName, String hdfsPath) throws Exception {
        Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
        log.info("存储层配置信息 : " + data_store_connect_attr);
        try (HdfsOperator operator = new HdfsOperator(FileNameUtils.normalize(Constant.STORECONFIGPATH + dataStoreConfBean.getDsl_name() + File.separator, true), data_store_connect_attr.get(StorageTypeKey.platform), data_store_connect_attr.get(StorageTypeKey.prncipal_name), data_store_connect_attr.get(StorageTypeKey.hadoop_user_name))) {
            if (!operator.exists(hdfsPath)) {
                if (!operator.mkdir(hdfsPath)) {
                    throw new AppSystemException("创建hdfs文件夹" + hdfsPath + "失败");
                }
            } else {
                if (!operator.deletePath(hdfsPath)) {
                    throw new AppSystemException("删除hdfs文件夹" + hdfsPath + "失败");
                }
                if (!operator.mkdir(hdfsPath)) {
                    throw new AppSystemException("创建hdfs文件夹" + hdfsPath + "失败");
                }
            }
            if (SystemUtil.OS_NAME.toLowerCase().contains("windows")) {
                log.info("开始上传文件到hdfs");
                if (!operator.upLoad(localFilePath, hdfsPath, true)) {
                    throw new AppSystemException("上传文件" + localFilePath + "到hdfs文件夹" + hdfsPath + "失败");
                }
                log.info("上传文件" + localFilePath + "到hdfs文件夹" + hdfsPath + "结束");
            } else {
                StringBuilder fsSql = new StringBuilder();
                fsSql.append("source /etc/profile;source ~/.bashrc;");
                if (!StringUtil.isEmpty(dataStoreConfBean.getData_store_layer_file().get(StorageTypeKey.keytab_file))) {
                    fsSql.append("kinit -k -t ").append(dataStoreConfBean.getData_store_layer_file().get(StorageTypeKey.keytab_file)).append(" ").append(data_store_connect_attr.get(StorageTypeKey.keytab_user)).append(" ").append(System.lineSeparator());
                }
                fsSql.append("hdfs dfs -put -f ").append(localFilePath).append(" ").append(hdfsPath).append(" ").append(System.lineSeparator());
                String hdfsShellFile = FileNameUtils.normalize(Constant.HDFSSHELLFILE + tableName + ".sh", true);
                FileUtils.write(new File(hdfsShellFile), fsSql.toString(), StandardCharsets.UTF_8);
                String command = "sh " + hdfsShellFile;
                log.info("开始运行(HDFS上传)>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + command);
                final CommandLine cmdLine = new CommandLine("sh");
                cmdLine.addArgument(hdfsShellFile);
                DefaultExecutor executor = new DefaultExecutor();
                ExecuteWatchdog watchdog = new ExecuteWatchdog(Integer.MAX_VALUE);
                executor.setWatchdog(watchdog);
                executor.execute(cmdLine);
                log.info("上传文件到" + hdfsPath + "结束");
            }
        }
    }

    @Override
    public void importDataToDatabase(AlgorithmsConf algorithmsConf, String hyFlag, DatabaseWrapper db) {
        if (hyFlag.equalsIgnoreCase("FD")) {
            ImportHyFdData.importDataToDatabase(algorithmsConf, db);
        } else if (hyFlag.equalsIgnoreCase("UCC")) {
            ImportHyUCCData.importDataToDatabase(algorithmsConf, db);
        }
    }
}
