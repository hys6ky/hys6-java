package hyren.serv6.agent.job.biz.core.dfstage;

import com.jcraft.jsch.ChannelSftp;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.SystemUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.dfstage.incrementfileprocess.TableProcessInterface;
import hyren.serv6.agent.job.biz.core.dfstage.incrementfileprocess.impl.MppTableProcessImpl;
import hyren.serv6.agent.job.biz.core.dfstage.service.ReadFileToDataBase;
import hyren.serv6.agent.job.biz.utils.FileUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.agent.job.biz.utils.SQLUtil;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.utils.jsch.FileProgressMonitor;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.hadoop.i.IHadoop;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.readfile.ReadFileToSolr;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.utils.ColUtil;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class DFUploadStageImpl extends AbstractJobStage {

    private final CollectTableBean collectTableBean;

    public DFUploadStageImpl(CollectTableBean collectTableBean) {
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        String storageTableName = collectTableBean.getStorage_table_name();
        log.info("------------------表" + storageTableName + "DB文件采集上传阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.UPLOAD.getCode());
        try {
            if (UnloadType.ZengLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                incrementCollect(stageParamInfo);
            } else if (UnloadType.QuanLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                fullAmountCollect(stageParamInfo);
            } else {
                throw new AppSystemException("表" + storageTableName + "DB文件采集指定的数据抽取卸数方式类型不正确");
            }
            log.info("------------------表" + storageTableName + "DB文件全量上传阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("表" + storageTableName + "DB文件采集上传阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.DBWenJian.getCode());
        return stageParamInfo;
    }

    private void incrementCollect(StageParamInfo stageParamInfo) {
        TableProcessInterface processInterface = null;
        try {
            List<DataStoreConfBean> dataStoreConfBeanList = collectTableBean.getDataStoreConfBean();
            for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                if (Store_type.DATABASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                    processInterface = new MppTableProcessImpl(stageParamInfo.getTableBean(), collectTableBean, dataStoreConfBean);
                } else {
                    throw new AppSystemException("增量采集目前没有实现其他数据库的增量更新代码");
                }
                for (String readFile : stageParamInfo.getFileArr()) {
                    processInterface.parserFileToTable(readFile);
                }
            }
        } catch (Exception e) {
            throw new AppSystemException("表" + collectTableBean.getStorage_table_name() + "db文件采集增量上传失败", e);
        } finally {
            if (processInterface != null) {
                processInterface.close();
            }
        }
    }

    private void fullAmountCollect(StageParamInfo stageParamInfo) {
        ExecutorService executor = null;
        try {
            List<DataStoreConfBean> dataStoreConfBeanList = collectTableBean.getDataStoreConfBean();
            for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                if (Store_type.DATABASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                    if (IsFlag.Shi.getCode().equals(dataStoreConfBean.getIs_hadoopclient())) {
                        execSftpToDbServer(dataStoreConfBean, stageParamInfo.getFileArr(), collectTableBean);
                    } else if (IsFlag.Fou.getCode().equals(dataStoreConfBean.getIs_hadoopclient())) {
                        executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
                        exeBatch(dataStoreConfBean, executor, stageParamInfo.getFileArr(), stageParamInfo.getTableBean());
                    } else {
                        throw new AppSystemException("错误的是否标识");
                    }
                } else if (Store_type.HIVE.getCode().equals(dataStoreConfBean.getStore_type())) {
                    dataStoreConfBean.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                    if (IsFlag.Shi.getCode().equals(dataStoreConfBean.getIs_hadoopclient())) {
                        execHDFSShell(dataStoreConfBean, stageParamInfo.getFileArr(), collectTableBean);
                    } else if (IsFlag.Fou.getCode().equals(dataStoreConfBean.getIs_hadoopclient())) {
                        executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
                        exeBatch(dataStoreConfBean, executor, stageParamInfo.getFileArr(), stageParamInfo.getTableBean());
                    } else {
                        throw new AppSystemException("错误的是否标识");
                    }
                } else if (Store_type.HBASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                    execHDFSShell(dataStoreConfBean, stageParamInfo.getFileArr(), collectTableBean);
                } else if (Store_type.SOLR.getCode().equals(dataStoreConfBean.getStore_type())) {
                    clearSolrData(dataStoreConfBean);
                    executor = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
                    exeBatchSolr(dataStoreConfBean, executor, stageParamInfo.getFileArr(), stageParamInfo.getTableBean());
                } else if (Store_type.ElasticSearch.getCode().equals(dataStoreConfBean.getStore_type())) {
                    log.warn("DB文件采集数据上传进ElasticSearch没有实现");
                } else if (Store_type.MONGODB.getCode().equals(dataStoreConfBean.getStore_type())) {
                    log.warn("DB文件采集数据上传进MONGODB没有实现");
                } else {
                    throw new AppSystemException("不支持的存储类型");
                }
            }
        } catch (Exception e) {
            throw new AppSystemException("表" + collectTableBean.getStorage_table_name() + "db文件采集全量上传失败", e);
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    private void clearSolrData(DataStoreConfBean dataStoreConfBean) {
        String configPath = FileNameUtils.normalize(Constant.STORECONFIGPATH + dataStoreConfBean.getDsl_name() + File.separator, true);
        Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
        SolrParam solrParam = new SolrParam();
        solrParam.setSolrZkUrl(data_store_connect_attr.get(StorageTypeKey.solr_zk_url));
        solrParam.setCollection(data_store_connect_attr.get(StorageTypeKey.collection));
        try (ISolrOperator os = SolrFactory.getSolrOperatorInstance(JobConstant.SOLRCLASSNAME, solrParam, configPath);
            SolrClient server = os.getSolrClient()) {
            String storageTableName = collectTableBean.getStorage_table_name();
            if (StorageType.ZhuiJia == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                String query = Constant._HYREN_S_DATE + ":" + collectTableBean.getEtlDate() + " and table-name:" + storageTableName;
                server.deleteByQuery(query);
            } else if (StorageType.TiHuan == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                String query = "table-name:" + storageTableName;
                server.deleteByQuery(query);
            } else {
                throw new AppSystemException("数据进solr不支持增量");
            }
        } catch (Exception e) {
            throw new AppSystemException("清理solr无效数据失败", e);
        }
    }

    private void exeBatchSolr(DataStoreConfBean dataStoreConfBean, ExecutorService executor, String[] fileArr, TableBean tableBean) {
        try {
            List<Future<Long>> list = new ArrayList<>();
            for (String fileAbsolutePath : fileArr) {
                ReadFileToSolr readFileToSolr = new ReadFileToSolr(fileAbsolutePath, tableBean, collectTableBean, dataStoreConfBean);
                Future<Long> submit = executor.submit(readFileToSolr);
                list.add(submit);
            }
            long count = 0;
            for (Future<Long> future : list) {
                count += future.get();
            }
            if (count < 0) {
                throw new AppSystemException("数据进" + dataStoreConfBean.getDsl_name() + "异常");
            }
            log.info("数据成功进" + dataStoreConfBean.getDsl_name() + ",总计进数" + count + "条");
        } catch (Exception e) {
            throw new AppSystemException("多线程读取文件batch进" + dataStoreConfBean.getDsl_name() + "异常", e);
        }
    }

    public static void execHDFSShell(DataStoreConfBean dataStoreConfBean, String[] localFiles, CollectTableBean collectTableBean) throws Exception {
        String hdfsPath = getUploadHdfsPath(collectTableBean);
        IHadoop iHadoop = ClassBase.hadoopInstance();
        for (String localFile : localFiles) {
            iHadoop.execHDFSShell(dataStoreConfBean, localFile, collectTableBean.getStorage_table_name(), hdfsPath);
        }
    }

    public static void execSftpToDbServer(DataStoreConfBean dataStoreConfBean, String[] localFiles, CollectTableBean collectTableBean) {
        Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
        try (SSHOperate sshOperate = new SSHOperate(new SSHDetails(data_store_connect_attr.get(StorageTypeKey.sftp_host), data_store_connect_attr.get(StorageTypeKey.sftp_user), data_store_connect_attr.get(StorageTypeKey.sftp_pwd), data_store_connect_attr.get(StorageTypeKey.sftp_port)), 0)) {
            log.info("==========文件上传开始=========");
            String database_type = data_store_connect_attr.get(StorageTypeKey.database_type);
            String targetPath;
            Dbtype dbType = ConnectionTool.getDbType(database_type);
            if (dbType == Dbtype.ORACLE) {
                targetPath = data_store_connect_attr.get(StorageTypeKey.external_root_path);
                String mkdirShell = "mkdir -p " + targetPath;
                sshOperate.execCommandBySSH(mkdirShell);
                log.info("==========上传文件的目标地址,创建目录：targetDir==========" + targetPath);
                String chmodShell = "chmod 777 " + targetPath;
                sshOperate.execCommandBySSH(chmodShell);
                uploadLobsFileToOracle(localFiles[0], sshOperate, targetPath, collectTableBean.getStorage_table_name());
            } else if (dbType == Dbtype.POSTGRESQL) {
                targetPath = getUploadServerPath(collectTableBean, data_store_connect_attr.get(StorageTypeKey.external_root_path));
                if (FileUtil.isSysDir(targetPath)) {
                    throw new AppSystemException("未知的异常或配置导致需要删除的目录是系统目录");
                }
                String deleteDirShell = "rm -rf " + targetPath;
                sshOperate.execCommandBySSH(deleteDirShell);
                log.info("==========上传文件的目标地址,目录存在先删除：targetDir==========" + deleteDirShell);
                String mkdirShell = "mkdir -p " + targetPath;
                sshOperate.execCommandBySSH(mkdirShell);
                log.info("==========上传文件的目标地址,创建目录：targetDir==========" + mkdirShell);
            } else {
                throw new AppSystemException(dataStoreConfBean.getDsl_name() + "数据库暂不支持外部表的形式入库");
            }
            for (String localFilePath : localFiles) {
                File file = new File(localFilePath);
                long fileSize = file.length();
                log.info("上传文件本地文件" + localFilePath + "到服务器" + targetPath);
                sshOperate.channelSftp.put(localFilePath, targetPath, new FileProgressMonitor(fileSize), ChannelSftp.OVERWRITE);
            }
            log.info("上传数据完成");
        } catch (Exception e) {
            throw new AppSystemException("上传文件失败", e);
        }
    }

    public static void uploadLobsFileToOracle(String absolutePath, SSHOperate sshOperate, String targetDir, String unload_hbase_name) throws Exception {
        String LOBs = FileNameUtils.getFullPath(absolutePath) + "LOBS" + File.separator;
        String[] fileNames = new File(LOBs).list();
        if (SystemUtil.OS_NAME.toLowerCase().contains("windows")) {
            if (fileNames != null) {
                for (String f : fileNames) {
                    sshOperate.channelSftp.put(LOBs + f, targetDir, ChannelSftp.OVERWRITE);
                }
            }
        } else {
            if (fileNames != null && fileNames.length > 0) {
                String zipFileName = LOBs + unload_hbase_name + ".zip";
                File zip = new File(zipFileName);
                if (zip.exists()) {
                    if (!zip.delete()) {
                        throw new AppSystemException("删除压缩文件失败");
                    }
                }
                String lobs_file = "find " + targetDir + " -name \"LOBs_" + unload_hbase_name + "_*\" | xargs rm -rf 'LOBs_" + unload_hbase_name + "_*'";
                if (FileUtil.isSysDir(targetDir)) {
                    throw new AppSystemException("异常导致需要删除的目录是系统目录");
                }
                sshOperate.execCommandBySSH(lobs_file);
                String zip_shell = "find " + LOBs + " -name '*' -print | zip -qj " + zipFileName + " -@";
                sshOperate.executeLocalShell(zip_shell);
                sshOperate.channelSftp.put(zipFileName, targetDir, ChannelSftp.OVERWRITE);
                sshOperate.execCommandBySSH("unzip " + targetDir + unload_hbase_name + ".zip" + " -d " + targetDir);
                sshOperate.execCommandBySSH("rm -rf " + targetDir + unload_hbase_name + ".zip");
            }
        }
    }

    private void exeBatch(DataStoreConfBean dataStoreConfBean, ExecutorService executor, String[] localFiles, TableBean tableBean) {
        long count = 0;
        List<Future<Long>> list = new ArrayList<>();
        Long storage_time = collectTableBean.getStorage_time();
        String storageTableName = collectTableBean.getStorage_table_name();
        String todayTableName = TableNameUtil.getUnderline1TableName(storageTableName, collectTableBean.getStorage_type(), storage_time);
        DatabaseWrapper db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr());
        try {
            if (storage_time > 0) {
                backupToDayTable(todayTableName, db);
                createTodayTable(tableBean, todayTableName, dataStoreConfBean, db);
            } else {
                if (StorageType.TiHuan == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    dropToDayTable(todayTableName, db);
                    createTodayTable(tableBean, todayTableName, dataStoreConfBean, db);
                } else if (StorageType.ZhuiJia == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    if (db.isExistTable(todayTableName)) {
                        if (db.getDbtype() == Dbtype.HIVE) {
                            DFDataLoadingStageImpl.backupTodayTableByAppend(todayTableName, tableBean, collectTableBean, db);
                        } else {
                            if (db.getDbtype() == Dbtype.KINGBASE) {
                                todayTableName = db.getDatabaseName() + '.' + todayTableName;
                            }
                            db.execute("DELETE FROM " + todayTableName + " WHERE " + Constant._HYREN_S_DATE + "='" + collectTableBean.getEtlDate() + "'");
                        }
                    } else {
                        createTodayTable(tableBean, todayTableName, dataStoreConfBean, db);
                    }
                } else if (StorageType.ZengLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.QuanLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.LiShiLaLian == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    backupToDayTable(todayTableName, db);
                    createTodayTable(tableBean, todayTableName, dataStoreConfBean, db);
                }
            }
            db.commit();
            for (String fileAbsolutePath : localFiles) {
                ReadFileToDataBase readFileToDataBase = new ReadFileToDataBase(fileAbsolutePath, tableBean, collectTableBean, dataStoreConfBean);
                Future<Long> submit = executor.submit(readFileToDataBase);
                list.add(submit);
            }
            for (Future<Long> future : list) {
                count += future.get();
            }
            backupPastTable(collectTableBean, db);
            db.commit();
            log.info("数据成功进入库" + dataStoreConfBean.getDsl_name() + "下的表" + storageTableName + ",总计进数" + count + "条");
        } catch (Exception e) {
            recoverBackupToDayTable(todayTableName, db);
            throw new AppSystemException("多线程读取文件batch进库" + dataStoreConfBean.getDsl_name() + "下的表" + storageTableName + "异常", e);
        } finally {
            db.close();
        }
    }

    public static void createTodayTable(TableBean tableBean, String todayTableName, DataStoreConfBean dataStoreConfBean, DatabaseWrapper db) {
        List<String> columns = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        List<String> types = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
        List<String> tarTypes = ColUtil.getTarTypes(tableBean, dataStoreConfBean.getDsl_id());
        StringBuilder sql = new StringBuilder(120);
        Dbtype dbtype = db.getDbtype();
        if (dbtype == Dbtype.TERADATA) {
            sql.append("CREATE MULTISET TABLE ");
        } else if (dbtype == Dbtype.KINGBASE) {
            sql.append("CREATE TABLE ").append(db.getDatabaseName()).append('.');
        } else {
            sql.append("CREATE TABLE ");
        }
        sql.append(todayTableName);
        log.info("==============columns==============" + columns);
        log.info("==============tarTypes==============" + tarTypes);
        log.info("==============types==============" + types);
        sql.append("(");
        SQLUtil.getSqlCond(columns, types, tarTypes, sql, dbtype);
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        db.execute(sql.toString());
    }

    public static String getUploadHdfsPath(CollectTableBean collectTableBean) {
        return FileNameUtils.normalize(JobConstant.PREFIX + File.separator + collectTableBean.getDatabase_id() + File.separator + collectTableBean.getStorage_table_name() + File.separator, true);
    }

    static String getUploadServerPath(CollectTableBean collectTableBean, String rootPath) {
        return FileNameUtils.normalize(rootPath + collectTableBean.getDatabase_id() + File.separator + collectTableBean.getStorage_table_name() + File.separator, true);
    }

    @Override
    public int getStageCode() {
        return StageConstant.UPLOAD.getCode();
    }
}
