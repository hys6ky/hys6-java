package hyren.serv6.b.batchcollection.cdcCollect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.batchcollection.cdcCollect.DataBaseUrlUtil.HostPort;
import hyren.serv6.b.batchcollection.cdcCollect.req.CDCTaskRunStatus;
import hyren.serv6.b.batchcollection.cdcCollect.req.CDCTaskRunStatus.Status;
import hyren.serv6.b.batchcollection.cdcCollect.req.Column;
import hyren.serv6.b.batchcollection.cdcCollect.req.FlinkCDCTable;
import hyren.serv6.b.batchcollection.cdcCollect.req.FlinkProducerParams;
import hyren.serv6.b.batchcollection.cdcCollect.req.JDBCData;
import hyren.serv6.b.batchcollection.cdcCollect.req.KafkaConsumerParams;
import hyren.serv6.b.batchcollection.cdcCollect.req.KafkaInfo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.JobExecuteState;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.StoreLayerDataSource;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.codes.TopicSource;
import hyren.serv6.base.entity.AgentInfo;
import hyren.serv6.base.entity.CollectJobClassify;
import hyren.serv6.base.entity.DataSource;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.entity.DataStoreReg;
import hyren.serv6.base.entity.DatabaseSet;
import hyren.serv6.base.entity.DtabRelationStore;
import hyren.serv6.base.entity.SdmTopicInfo;
import hyren.serv6.base.entity.TableCdcJobInfo;
import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.entity.TableInfo;
import hyren.serv6.base.entity.TableStorageInfo;
import hyren.serv6.base.entity.TbcolSrctgtMap;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.http.HttpClient.ResponseValue;
import hyren.serv6.commons.utils.AgentActionUtil;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class CdcCollectService {

    private static final String UPDATESTATE_COLLECTRUN = "collectRun";

    private static final String UPDATESTATE_COLLECTFAILED = "collectFailed";

    private static final String UPDATESTATE_SYNCRUN = "syncRun";

    private static final String UPDATESTATE_SYNCFAILED = "syncFailed";

    private static final String UPDATESTATE_COLLECTABORT = "collectAbort";

    private static final String UPDATESTATE_SYNCABORT = "syncAbort";

    public String execute(Long taskId) {
        String selectTask = "select * from " + DatabaseSet.TableName + " where database_id = ? ";
        DatabaseSet databaseSet = Dbo.queryOneObject(Dbo.db(), DatabaseSet.class, selectTask, taskId).get();
        Validator.notNull(databaseSet, "未找到相关任务");
        String selectAgent = "select * from " + AgentInfo.TableName + " where agent_id = ? ";
        AgentInfo agentInfo = Dbo.queryOneObject(Dbo.db(), AgentInfo.class, selectAgent, databaseSet.getAgent_id()).get();
        Validator.notNull(agentInfo, "未找到 Agent");
        String selectTable = "select * from " + TableInfo.TableName + " where database_id = ? ";
        List<TableInfo> tableList = Dbo.queryList(Dbo.db(), TableInfo.class, selectTable, taskId);
        Validator.notEmpty(tableList, "未找到表信息");
        String kafkaAddress = PropertyParaValue.getString("kafka_address", "");
        Validator.notEmpty(kafkaAddress, "系统参数中未配置 kafka_address，请联系管理员");
        HttpClient.ResponseValue resValue = null;
        List<String> tableNames = tableList.stream().map(t -> t.getTable_name()).collect(Collectors.toList());
        String tableNamesStr = String.join(",", tableNames);
        String url = "http://" + agentInfo.getAgent_ip() + ":" + agentInfo.getAgent_port() + "/agent" + AgentActionUtil.CDC_EXECUTE;
        log.info(url);
        resValue = new HttpClient().addData("taskId", taskId).addData("tableNames", tableNamesStr).post(url);
        if (resValue != null && resValue.getCode() == 200) {
            ActionResult ar = ActionResult.toActionResult(resValue.getBodyString());
            if (ar.getCode() == 999) {
                for (TableInfo ti : tableList) {
                    String tsiSql = "select * from " + TableStorageInfo.TableName + " where TABLE_ID = ? ";
                    TableStorageInfo tableStorageInfo = Dbo.queryOneObject(TableStorageInfo.class, tsiSql, ti.getTable_id()).get();
                    String dslSql = "select dsl.* from " + DataStoreLayer.TableName + " dsl " + "left join " + DtabRelationStore.TableName + " drs on dsl.dsl_id = drs.dsl_id where drs.tab_id = ? " + "and dsl.store_type = ?";
                    DataStoreLayer dsl = Dbo.queryOneObject(DataStoreLayer.class, dslSql, tableStorageInfo.getStorage_id(), Store_type.KAFKA.getCode()).orElse(null);
                    if (dsl != null && Store_type.KAFKA == Store_type.ofEnumByCode(dsl.getStore_type())) {
                        String topicSql = "select count(1) from " + SdmTopicInfo.TableName + " where sdm_top_name = ? ";
                        long count = Dbo.queryNumber(topicSql, tableStorageInfo.getHyren_name()).getAsLong();
                        if (count == 0) {
                            SdmTopicInfo sdmTopicInfo = new SdmTopicInfo();
                            sdmTopicInfo.setTopic_id(PrimayKeyGener.getNextId());
                            sdmTopicInfo.setSdm_top_name(tableStorageInfo.getHyren_name());
                            sdmTopicInfo.setSdm_top_cn_name(tableStorageInfo.getHyren_name());
                            String attrSql = "select * from " + DataStoreLayerAttr.TableName + " where dsl_id = ? ";
                            List<DataStoreLayerAttr> attrList = Dbo.queryList(DataStoreLayerAttr.class, attrSql, dsl.getDsl_id());
                            for (DataStoreLayerAttr attr : attrList) {
                                if (attr.getStorage_property_key().equals("kafka_broker_s")) {
                                    sdmTopicInfo.setSdm_bstp_serv(attr.getStorage_property_val());
                                } else if (attr.getStorage_property_key().equals("kafka_zk_url")) {
                                    sdmTopicInfo.setSdm_zk_host(attr.getStorage_property_val());
                                }
                            }
                            sdmTopicInfo.setSdm_partition(1L);
                            sdmTopicInfo.setSdm_replication(1L);
                            sdmTopicInfo.setCreate_date(DateUtil.getSysDate());
                            sdmTopicInfo.setCreate_time(DateUtil.getSysTime());
                            sdmTopicInfo.setUser_id(UserUtil.getUserId());
                            sdmTopicInfo.setTopic_source(TopicSource.CDC.getCode());
                            sdmTopicInfo.add(Dbo.db());
                        }
                    }
                }
            } else {
                throw new BusinessException(ar.getMessage());
            }
        } else {
            throw new BusinessException("链接 Agent 失败，请联系管理员");
        }
        return resValue.getBodyString();
    }

    public KafkaConsumerParams getSyncParam(Long taskId, String tableName) {
        KafkaConsumerParams param = new KafkaConsumerParams();
        param.setTaskId(taskId);
        String databaseSql = "select * from " + DatabaseSet.TableName + " where database_id =?";
        DatabaseSet databaseSet = Dbo.queryOneObject(DatabaseSet.class, databaseSql, taskId).orElse(null);
        String dataSourceSql = "select ds.* from " + AgentInfo.TableName + " ai inner join " + DataSource.TableName + " ds on ai.source_id  = ds.source_id inner join " + DatabaseSet.TableName + " dsset on ai.agent_id = dsset.agent_id" + " where dsset.database_id = ?";
        DataSource dataSource = Dbo.queryOneObject(DataSource.class, dataSourceSql, taskId).orElse(null);
        Validator.notNull(dataSource, "未找到数据源");
        String classifySql = "select * from " + CollectJobClassify.TableName + " where CLASSIFY_ID = ?";
        CollectJobClassify classify = Dbo.queryOneObject(CollectJobClassify.class, classifySql, databaseSet.getClassify_id()).orElse(null);
        Validator.notNull(classify, "未找到分类");
        String topic = dataSource.getDatasource_number() + "_" + classify.getClassify_num() + "_" + tableName;
        param.setTopic(topic);
        String tableSql = "select * from " + TableInfo.TableName + " where DATABASE_ID = ? and TABLE_NAME = ?";
        TableInfo table = Dbo.queryOneObject(TableInfo.class, tableSql, taskId, tableName).orElse(null);
        Validator.notNull(table, "未找到表");
        String tableStorageInfoSql = "select * from table_storage_info where table_id = ?";
        TableStorageInfo tableStorageInfo = Dbo.queryOneObject(TableStorageInfo.class, tableStorageInfoSql, table.getTable_id()).orElse(null);
        param.setStorageType(StorageType.ofEnumByCode(tableStorageInfo.getStorage_type()));
        String drsSql = "select drs.dsl_id from " + DtabRelationStore.TableName + " drs where drs.tab_id = ? and drs.data_source = ?";
        List<Map<String, Object>> drsList = Dbo.queryList(drsSql, tableStorageInfo.getStorage_id(), StoreLayerDataSource.DB.getCode());
        Validator.notEmpty(drsSql, "未找到目标库信息");
        param.setJdbcDatas(new ArrayList<JDBCData>());
        for (Map<String, Object> map : drsList) {
            Object dslId = map.get("dsl_id");
            String dslSql = "select * from " + DataStoreLayer.TableName + " where dsl_id = ? ";
            DataStoreLayer dsl = Dbo.queryOneObject(DataStoreLayer.class, dslSql, dslId).orElse(null);
            Validator.notNull(dsl, "未找到存储层信息");
            if (Store_type.KAFKA.equals(Store_type.ofEnumByCode(dsl.getStore_type()))) {
                continue;
            }
            JDBCData jdbc = new JDBCData();
            Validator.notNull(table, "未查询到指定表数据");
            jdbc.setBefore_table_name(tableName);
            jdbc.setAfter_table_name(tableStorageInfo.getHyren_name());
            String jdbcSql = "select * from " + DataStoreLayerAttr.TableName + " where dsl_id = ?";
            List<DataStoreLayerAttr> attrs = Dbo.queryList(DataStoreLayerAttr.class, jdbcSql, dslId);
            for (DataStoreLayerAttr attr : attrs) {
                if (attr.getStorage_property_key().equals("database_driver")) {
                    jdbc.setDatabase_drive(attr.getStorage_property_val());
                } else if (attr.getStorage_property_key().equals("jdbc_url")) {
                    jdbc.setJdbc_url(attr.getStorage_property_val());
                } else if (attr.getStorage_property_key().equals("user_name")) {
                    jdbc.setUser_name(attr.getStorage_property_val());
                    ;
                } else if (attr.getStorage_property_key().equals("database_pwd")) {
                    jdbc.setDatabase_pad(attr.getStorage_property_val());
                } else if (attr.getStorage_property_key().equals("database_type")) {
                    jdbc.setDatabase_type(attr.getStorage_property_val());
                } else if (attr.getStorage_property_key().equals("database_name")) {
                    jdbc.setDatabase_name(attr.getStorage_property_val());
                }
            }
            String columnSql = "select * from " + TableColumn.TableName + " tc left join " + TbcolSrctgtMap.TableName + " tsm on tc.column_id =tsm.column_id where tc.table_id = ? and tsm.dsl_id = ?";
            List<Map<String, Object>> temp_columns = Dbo.queryList(columnSql, table.getTable_id(), dslId);
            Validator.notEmpty(temp_columns, "未查询到字段数据");
            List<Column> columnList = temp_columns.stream().map(c -> {
                return new Column(c.get("column_name").toString(), c.get("column_type").toString(), c.get("column_tar_type").toString(), IsFlag.ofEnumByCode(c.get("is_primary_key").toString()));
            }).collect(Collectors.toList());
            jdbc.setColumns(columnList);
            ;
            param.getJdbcDatas().add(jdbc);
        }
        String kafkaAddress = PropertyParaValue.getString("kafka_address", "");
        Validator.notEmpty(kafkaAddress, "未找到kafka地址");
        param.setKafka_servers(kafkaAddress.split(",")[0]);
        param.setGroup_id("hyren");
        return param;
    }

    public FlinkProducerParams getCollectParam(Long taskId, String[] tableNames) {
        FlinkProducerParams params = new FlinkProducerParams();
        String dslSql = "select dsl.* from " + DatabaseSet.TableName + " ds inner join " + DataStoreLayer.TableName + " dsl on ds.dsl_id=dsl.dsl_id where ds.database_id = ?";
        DataStoreLayer dataStoreLayer = Dbo.queryOneObject(DataStoreLayer.class, dslSql, taskId).orElse(null);
        Validator.notNull(dataStoreLayer, "未查询到来源存储层");
        String jdbcSql = "select * from " + DataStoreLayerAttr.TableName + " where dsl_id = ?";
        List<DataStoreLayerAttr> attrList = Dbo.queryList(DataStoreLayerAttr.class, jdbcSql, dataStoreLayer.getDsl_id());
        String jdbc_url = null;
        for (DataStoreLayerAttr attr : attrList) {
            if (attr.getStorage_property_key().equals("jdbc_url")) {
                jdbc_url = attr.getStorage_property_val();
            } else if (attr.getStorage_property_key().equals("schema")) {
                params.setDatabase_schema(attr.getStorage_property_val());
            } else if (attr.getStorage_property_key().equals("user_name")) {
                params.setDatabase_username(attr.getStorage_property_val());
            } else if (attr.getStorage_property_key().equals("database_pwd")) {
                params.setDatabase_password(attr.getStorage_property_val());
            } else if (attr.getStorage_property_key().equals("database_type")) {
                params.setDatabase_type(attr.getStorage_property_val());
            } else if (attr.getStorage_property_key().equals("database_name")) {
                params.setDatabase_name(attr.getStorage_property_val());
            }
        }
        Validator.notEmpty(jdbc_url, "未找到数据库url");
        HostPort hp = DataBaseUrlUtil.getHostAndPortByUrl(jdbc_url, params.getDatabase_type());
        params.setDatabase_ip(hp.getHost());
        params.setDatabase_port(hp.getPort());
        Validator.notEmpty(params.getDatabase_ip(), "未找到数据库 ip");
        Validator.isTrue(params.getDatabase_port() != -1, "未找到数据库端口");
        List<FlinkCDCTable> tableList = new ArrayList<FlinkCDCTable>();
        String sql = "select dsl.* from " + TableInfo.TableName + " ti " + "join " + TableStorageInfo.TableName + " tsi on ti.table_id = tsi.table_id " + "join " + DtabRelationStore.TableName + " drs on tsi.storage_id = drs.tab_id " + "join " + DataStoreLayer.TableName + " dsl on drs.dsl_id = dsl.dsl_id " + "where ti.table_name = ? and ti.database_id = ?";
        for (String tableName : tableNames) {
            FlinkCDCTable table = new FlinkCDCTable();
            List<DataStoreLayer> dslList = Dbo.queryList(DataStoreLayer.class, sql, tableName, taskId);
            Validator.notEmpty(dslList, "未找到 " + tableName + " 的目标存储层");
            for (DataStoreLayer dsl : dslList) {
                KafkaInfo info = new KafkaInfo();
                if (Store_type.KAFKA == Store_type.ofEnumByCode(dsl.getStore_type())) {
                    String attrSql = "select * from " + DataStoreLayerAttr.TableName + " where dsl_id = ? ";
                    List<DataStoreLayerAttr> goalAttrList = Dbo.queryList(DataStoreLayerAttr.class, attrSql, dataStoreLayer.getDsl_id());
                    for (DataStoreLayerAttr attr : goalAttrList) {
                        if (attr.getStorage_property_key().equals("kafka_broker_s")) {
                            info.setKafka_servers(attr.getStorage_property_val());
                        } else if (attr.getStorage_property_key().equals("kafka_username")) {
                            info.setKafka_username(attr.getStorage_property_val());
                        } else if (attr.getStorage_property_key().equals("kafka_password")) {
                            info.setKafka_password(attr.getStorage_property_val());
                        } else if (attr.getStorage_property_key().equals("kafka_key_serializer")) {
                            info.setKafka_key_serializer(attr.getStorage_property_val());
                        } else if (attr.getStorage_property_key().equals("kafka_value_serializer")) {
                            info.setKafka_value_serializer(attr.getStorage_property_val());
                        } else if (attr.getStorage_property_key().equals("kafka_security_protocol")) {
                            info.setKafka_security_protocol(attr.getStorage_property_val());
                        } else if (attr.getStorage_property_key().equals("kafka_sasl_mechanism")) {
                            info.setKafka_sasl_mechanism(attr.getStorage_property_val());
                        }
                    }
                } else {
                    String kafkaAddress = PropertyParaValue.getString("kafka_address", "");
                    info.setKafka_servers(kafkaAddress);
                }
                table.addKafka(info);
            }
            table.setTable_name(tableName);
            String databaseSql = "select * from " + DatabaseSet.TableName + " where database_id =?";
            DatabaseSet databaseSet = Dbo.queryOneObject(DatabaseSet.class, databaseSql, taskId).orElse(null);
            String dataSourceSql = "select ds.* from " + AgentInfo.TableName + " ai inner join " + DataSource.TableName + " ds on ai.source_id  = ds.source_id where ai.agent_id = ?";
            DataSource dataSource = Dbo.queryOneObject(DataSource.class, dataSourceSql, databaseSet.getAgent_id()).orElse(null);
            Validator.notNull(dataSource, "未找到数据源");
            String classifySql = "select * from " + CollectJobClassify.TableName + " where CLASSIFY_ID = ?";
            CollectJobClassify classify = Dbo.queryOneObject(CollectJobClassify.class, classifySql, databaseSet.getClassify_id()).orElse(null);
            Validator.notNull(classify, "未找到分类");
            table.setTopic(dataSource.getDatasource_number() + "_" + classify.getClassify_num() + "_" + tableName);
            tableList.add(table);
            if (StringUtil.isEmpty(params.getLastJobId())) {
                String cdcInfoSql = "select tcji.* from " + TableCdcJobInfo.TableName + " tcji join " + TableInfo.TableName + " ti on tcji.table_id = ti.table_id where ti.database_id = ? and ti.table_name =?";
                List<TableCdcJobInfo> cdcJobList = Dbo.queryList(TableCdcJobInfo.class, cdcInfoSql, taskId, tableName);
                if (cdcJobList != null && cdcJobList.size() > 0) {
                    params.setLastJobId(cdcJobList.get(0).getFlink_job_id());
                }
            }
        }
        params.setTables(tableList);
        return params;
    }

    public int upateState(String type, Long taskId, String[] tableNameArr, Long pid, String jobId, String date, String time) {
        int executeNum = 0;
        StringJoiner in = new StringJoiner(",");
        Arrays.stream(tableNameArr).forEach(table -> {
            in.add("'" + table + "'");
        });
        String tableSql = "select * from " + TableInfo.TableName + " where DATABASE_ID = ? and TABLE_NAME in (" + in + ") ";
        List<TableInfo> tableList = Dbo.queryList(TableInfo.class, tableSql, taskId);
        for (TableInfo table : tableList) {
            String cdcSql = "select * from " + TableCdcJobInfo.TableName + " where table_id = ?";
            TableCdcJobInfo jobInfo = Dbo.queryOneObject(TableCdcJobInfo.class, cdcSql, table.getTable_id()).orElse(null);
            if (jobInfo == null) {
                jobInfo = new TableCdcJobInfo();
                jobInfo.setTable_id(table.getTable_id());
                jobInfo.setCdc_job_id(PrimayKeyGener.getNextId());
                jobInfo.setCsm_job_status(JobExecuteState.DengDai.getCode());
                jobInfo.setSync_job_status(JobExecuteState.DengDai.getCode());
                jobInfo.add(Dbo.db());
            }
            String update = "";
            List<Object> params = new ArrayList<Object>();
            if (type.equals(UPDATESTATE_COLLECTRUN)) {
                update = "UPDATE " + TableCdcJobInfo.TableName + " SET sync_job_status=?, sync_job_pid=?, sync_job_s_date=?, sync_job_s_time=?, flink_job_id=? WHERE table_id = ?";
                params.add(JobExecuteState.YunXing.getCode());
                params.add(pid);
                params.add(date);
                params.add(time);
                params.add(jobId);
                params.add(table.getTable_id());
            } else if (type.equals(UPDATESTATE_COLLECTFAILED)) {
                update = "UPDATE " + TableCdcJobInfo.TableName + " SET sync_job_status=?, sync_job_e_date=?, sync_job_e_time=? WHERE table_id = ?";
                params.add(JobExecuteState.ShiBai.getCode());
                params.add(date);
                params.add(time);
                params.add(table.getTable_id());
            } else if (type.equals(UPDATESTATE_COLLECTABORT)) {
                update = "UPDATE " + TableCdcJobInfo.TableName + " SET sync_job_status=?, sync_job_e_date=?, sync_job_e_time=? WHERE table_id = ?";
                params.add(JobExecuteState.ZhongZhi.getCode());
                params.add(date);
                params.add(time);
                params.add(table.getTable_id());
            } else if (type.equals(UPDATESTATE_SYNCRUN)) {
                update = "UPDATE " + TableCdcJobInfo.TableName + " SET csm_job_status=?, csm_job_pid=?, csm_job_s_date=?, csm_job_s_time=? WHERE table_id = ?";
                params.add(JobExecuteState.YunXing.getCode());
                params.add(pid);
                params.add(date);
                params.add(time);
                params.add(table.getTable_id());
            } else if (type.equals(UPDATESTATE_SYNCFAILED)) {
                update = "UPDATE " + TableCdcJobInfo.TableName + " SET csm_job_status=?, csm_job_e_date=?, csm_job_e_time=? WHERE table_id = ?";
                params.add(JobExecuteState.ShiBai.getCode());
                params.add(date);
                params.add(time);
                params.add(table.getTable_id());
            } else if (type.equals(UPDATESTATE_SYNCABORT)) {
                update = "UPDATE " + TableCdcJobInfo.TableName + " SET csm_job_status=?, csm_job_e_date=?, csm_job_e_time=? WHERE table_id = ?";
                params.add(JobExecuteState.ZhongZhi.getCode());
                params.add(date);
                params.add(time);
                params.add(table.getTable_id());
            } else {
                throw new BusinessException("修改flink信息没有找到对应的处理程序");
            }
            int execute = Dbo.execute(Dbo.db(), update, params);
            if (execute > 0) {
                executeNum++;
            }
        }
        return executeNum;
    }

    public Boolean addDataStoreReg(Long taskId, String tableName, String date, String time) {
        DataStoreReg reg = new DataStoreReg();
        DatabaseWrapper db = Dbo.db();
        AgentInfo agentInfo = Dbo.queryOneObject(db, AgentInfo.class, "select ai.* from " + AgentInfo.TableName + " ai left join " + DatabaseSet.TableName + " ds on ai.agent_id = ds.agent_id where ds.database_id = ?", taskId).orElseThrow(() -> new BusinessException("未找到Agent信息"));
        List<TableInfo> tableInfos = Dbo.queryList(db, TableInfo.class, "select * from " + TableInfo.TableName + " where database_id = ?", taskId);
        StringJoiner tableIds = new StringJoiner(",");
        for (TableInfo table : tableInfos) {
            TableStorageInfo tableStorageInfo = Dbo.queryOneObject(db, TableStorageInfo.class, "select * from table_storage_info where table_id = ?", table.getTable_id()).orElseThrow(() -> new BusinessException("未找到目标表信息"));
            String sql = "select count(*) from " + DataStoreReg.TableName + " where COLLECT_TYPE = ? " + "and HYREN_NAME = ? " + "and AGENT_ID = ? " + "and SOURCE_ID = ? " + "and DATABASE_ID = ? " + "and TABLE_ID = ?";
            long count = Dbo.queryNumber(db, sql, agentInfo.getAgent_type(), tableStorageInfo.getHyren_name(), agentInfo.getAgent_id(), agentInfo.getSource_id(), taskId, table.getTable_id()).getAsLong();
            if (count <= 0) {
                reg.setAgent_id(agentInfo.getAgent_id());
                reg.setCollect_type(agentInfo.getAgent_type());
                reg.setDatabase_id(taskId);
                reg.setFile_id(UUID.randomUUID().toString());
                reg.setFile_size(0L);
                reg.setHyren_name(tableStorageInfo.getHyren_name());
                reg.setTable_name(tableName);
                reg.setOriginal_name(table.getTable_ch_name());
                reg.setOriginal_update_date(date);
                reg.setOriginal_update_time(time);
                reg.setSource_id(agentInfo.getSource_id());
                reg.setStorage_date(date);
                reg.setStorage_time(time);
                reg.setTable_id(table.getTable_id());
                reg.setMeta_info("");
                reg.add(db);
            }
        }
        return true;
    }

    public CDCTaskRunStatus getRunStatusByTaskId(Long taskId) {
        CDCTaskRunStatus status = new CDCTaskRunStatus();
        String taskSql = "select * from " + DatabaseSet.TableName + " where database_id = ? ";
        DatabaseSet databaseSet = Dbo.queryOneObject(Dbo.db(), DatabaseSet.class, taskSql, taskId).get();
        Validator.notNull(databaseSet, "未找到相关任务");
        String agentSql = "select * from " + AgentInfo.TableName + " where agent_id = ? ";
        AgentInfo agentInfo = Dbo.queryOneObject(Dbo.db(), AgentInfo.class, agentSql, databaseSet.getAgent_id()).get();
        Validator.notNull(agentInfo, "未找到 Agent");
        String collectTableSql = "select * from " + TableInfo.TableName + " where database_id = ?";
        List<TableInfo> tableInfoList = Dbo.queryList(TableInfo.class, collectTableSql, taskId);
        Set<String> pids = new HashSet<String>();
        for (TableInfo tableInfo : tableInfoList) {
            String collectJobInfoSql = "select * from " + TableCdcJobInfo.TableName + " where table_id = ? and sync_job_pid is not null";
            List<TableCdcJobInfo> collectJobList = Dbo.queryList(TableCdcJobInfo.class, collectJobInfoSql, tableInfo.getTable_id());
            for (TableCdcJobInfo job : collectJobList) {
                if (!pids.contains(job.getSync_job_pid().toString())) {
                    status.getCollectTaskList().add(new Status(job.getSync_job_pid(), null, null));
                }
                pids.add(job.getSync_job_pid().toString());
            }
            String syncJobInfoSql = "select * from " + TableCdcJobInfo.TableName + " where table_id = ? and csm_job_pid is not null";
            List<TableCdcJobInfo> syncJobList = Dbo.queryList(TableCdcJobInfo.class, syncJobInfoSql, tableInfo.getTable_id());
            for (TableCdcJobInfo job : syncJobList) {
                pids.add(job.getCsm_job_pid().toString());
                status.getSyncTaskList().add(new Status(job.getCsm_job_pid(), tableInfo.getTable_name(), null));
            }
        }
        if (pids.size() != 0) {
            String url = "http://" + agentInfo.getAgent_ip() + ":" + agentInfo.getAgent_port() + "/agent" + AgentActionUtil.CDC_STATUS;
            log.info(url);
            ResponseValue response = new HttpClient().addData("pIds", String.join(",", pids.stream().filter(id -> !id.equals("0")).collect(Collectors.toList()))).post(url);
            Map<String, Boolean> map = null;
            if (response != null && response.getCode() == 200) {
                ActionResult ar = ActionResult.toActionResult(response.getBodyString());
                if (ar.getCode() == 999) {
                    Object data = ar.getData();
                    try {
                        map = (Map<String, Boolean>) data;
                    } catch (ClassCastException e) {
                        throw new BusinessException("Agent 返回参数异常");
                    }
                } else {
                    throw new BusinessException(ar.getMessage());
                }
            } else {
                throw new BusinessException("Agent 链接异常");
            }
            for (Status s : status.getCollectTaskList()) {
                s.setStatus(map.get(s.getPId().toString()));
            }
            for (Status s : status.getSyncTaskList()) {
                s.setStatus(map.get(s.getPId().toString()));
            }
        } else {
            log.info("程序未启动过");
        }
        return status;
    }

    public String startCollectTask(Long taskId) {
        String taskSql = "select * from " + DatabaseSet.TableName + " where database_id = ? ";
        DatabaseSet databaseSet = Dbo.queryOneObject(Dbo.db(), DatabaseSet.class, taskSql, taskId).get();
        Validator.notNull(databaseSet, "未找到相关任务");
        String agentSql = "select * from " + AgentInfo.TableName + " where agent_id = ? ";
        AgentInfo agentInfo = Dbo.queryOneObject(Dbo.db(), AgentInfo.class, agentSql, databaseSet.getAgent_id()).get();
        Validator.notNull(agentInfo, "未找到 Agent");
        StringJoiner tableNameJoiner = new StringJoiner(",");
        String selectTable = "select * from " + TableInfo.TableName + " where database_id = ? ";
        List<TableInfo> tableList = Dbo.queryList(Dbo.db(), TableInfo.class, selectTable, taskId);
        Validator.notEmpty(tableList, "未找到表信息");
        tableList.stream().forEach(table -> tableNameJoiner.add(table.getTable_name()));
        String url = "http://" + agentInfo.getAgent_ip() + ":" + agentInfo.getAgent_port() + "/agent" + AgentActionUtil.START_CDC_COLLECT_PROGRAM;
        log.info(url);
        ResponseValue response = new HttpClient().addData("taskId", taskId).addData("tableNames", tableNameJoiner.toString()).post(url);
        if (response != null && response.getCode() == 200) {
            ActionResult ar = ActionResult.toActionResult(response.getBodyString());
            if (ar.getCode() == 999) {
                if (Boolean.TRUE.equals(ar.getData())) {
                    return "启动成功";
                } else {
                    throw new BusinessException("远程启动失败");
                }
            } else {
                throw new BusinessException(ar.getMessage());
            }
        } else {
            throw new BusinessException("Agent 链接异常");
        }
    }

    public String stopCollectTask(Long taskId) {
        CDCTaskRunStatus status = new CDCTaskRunStatus();
        String taskSql = "select * from " + DatabaseSet.TableName + " where database_id = ? ";
        DatabaseSet databaseSet = Dbo.queryOneObject(Dbo.db(), DatabaseSet.class, taskSql, taskId).get();
        Validator.notNull(databaseSet, "未找到相关任务");
        String agentSql = "select * from " + AgentInfo.TableName + " where agent_id = ? ";
        AgentInfo agentInfo = Dbo.queryOneObject(Dbo.db(), AgentInfo.class, agentSql, databaseSet.getAgent_id()).get();
        Validator.notNull(agentInfo, "未找到 Agent");
        String collectTableSql = "select * from " + TableInfo.TableName + " where database_id = ?";
        List<TableInfo> tableInfoList = Dbo.queryList(TableInfo.class, collectTableSql, taskId);
        Set<String> pids = new HashSet<String>();
        List<String> tableNameList = new ArrayList<String>();
        for (TableInfo tableInfo : tableInfoList) {
            tableNameList.add(tableInfo.getTable_name());
            String collectJobInfoSql = "select * from " + TableCdcJobInfo.TableName + " where table_id = ? and sync_job_pid is not null";
            List<TableCdcJobInfo> collectJobList = Dbo.queryList(TableCdcJobInfo.class, collectJobInfoSql, tableInfo.getTable_id());
            for (TableCdcJobInfo job : collectJobList) {
                pids.add(job.getSync_job_pid().toString());
                status.getCollectTaskList().add(new Status(job.getSync_job_pid(), tableInfo.getTable_name(), null));
            }
        }
        String url = "http://" + agentInfo.getAgent_ip() + ":" + agentInfo.getAgent_port() + "/agent" + AgentActionUtil.CDC_ABORT;
        log.info(url);
        ResponseValue response = new HttpClient().addData("pIds", String.join(",", pids)).post(url);
        if (response != null && response.getCode() == 200) {
            ActionResult ar = ActionResult.toActionResult(response.getBodyString());
            if (ar.getCode() == 999) {
                Object data = ar.getData();
                if (Boolean.TRUE.equals(data)) {
                    this.upateState(UPDATESTATE_COLLECTABORT, taskId, tableNameList.stream().toArray(String[]::new), null, null, DateUtil.getSysDate(), DateUtil.getSysTime());
                    return "停止成功";
                } else {
                    throw new BusinessException("Agent 返回数据异常");
                }
            } else {
                throw new BusinessException(ar.getMessage());
            }
        } else {
            throw new BusinessException("Agent 链接异常");
        }
    }

    public String startSyncTask(Long taskId, String tableName) {
        String taskSql = "select * from " + DatabaseSet.TableName + " where database_id = ? ";
        DatabaseSet databaseSet = Dbo.queryOneObject(Dbo.db(), DatabaseSet.class, taskSql, taskId).get();
        Validator.notNull(databaseSet, "未找到相关任务");
        String agentSql = "select * from " + AgentInfo.TableName + " where agent_id = ? ";
        AgentInfo agentInfo = Dbo.queryOneObject(Dbo.db(), AgentInfo.class, agentSql, databaseSet.getAgent_id()).get();
        Validator.notNull(agentInfo, "未找到 Agent");
        StringJoiner tableNameJoiner = new StringJoiner(",");
        String selectTable = "select * from " + TableInfo.TableName + " where database_id = ? and table_name = ?";
        List<TableInfo> tableList = Dbo.queryList(Dbo.db(), TableInfo.class, selectTable, taskId, tableName);
        Validator.notEmpty(tableList, "未找到表信息");
        tableList.stream().forEach(table -> tableNameJoiner.add(table.getTable_name()));
        String url = "http://" + agentInfo.getAgent_ip() + ":" + agentInfo.getAgent_port() + "/agent" + AgentActionUtil.START_CDC_SYNC_PROGRAM;
        log.info(url);
        ResponseValue response = new HttpClient().addData("taskId", taskId).addData("tableNames", tableNameJoiner.toString()).post(url);
        if (response != null && response.getCode() == 200) {
            ActionResult ar = ActionResult.toActionResult(response.getBodyString());
            if (ar.getCode() == 999) {
                if (Boolean.TRUE.equals(ar.getData())) {
                    return "启动成功";
                } else {
                    throw new BusinessException("远程启动失败");
                }
            } else {
                throw new BusinessException(ar.getMessage());
            }
        } else {
            throw new BusinessException("Agent 链接异常");
        }
    }

    public String stopSyncTask(Long taskId, String tableName) {
        String taskSql = "select * from " + DatabaseSet.TableName + " where database_id = ? ";
        DatabaseSet databaseSet = Dbo.queryOneObject(Dbo.db(), DatabaseSet.class, taskSql, taskId).get();
        Validator.notNull(databaseSet, "未找到相关任务");
        String agentSql = "select * from " + AgentInfo.TableName + " where agent_id = ? ";
        AgentInfo agentInfo = Dbo.queryOneObject(Dbo.db(), AgentInfo.class, agentSql, databaseSet.getAgent_id()).get();
        Validator.notNull(agentInfo, "未找到 Agent");
        String collectTableSql = "select * from " + TableInfo.TableName + " where database_id = ? and table_name = ?";
        List<TableInfo> tableInfoList = Dbo.queryList(TableInfo.class, collectTableSql, taskId, tableName);
        Set<String> pids = new HashSet<String>();
        for (TableInfo tableInfo : tableInfoList) {
            String collectJobInfoSql = "select * from " + TableCdcJobInfo.TableName + " where table_id = ? and csm_job_status = ? and csm_job_pid is not null";
            List<TableCdcJobInfo> collectJobList = Dbo.queryList(TableCdcJobInfo.class, collectJobInfoSql, tableInfo.getTable_id(), JobExecuteState.YunXing.getCode());
            for (TableCdcJobInfo job : collectJobList) {
                pids.add(job.getCsm_job_pid().toString());
            }
        }
        String url = "http://" + agentInfo.getAgent_ip() + ":" + agentInfo.getAgent_port() + "/agent" + AgentActionUtil.CDC_ABORT;
        log.info(url);
        ResponseValue response = new HttpClient().addData("pIds", String.join(",", pids)).post(url);
        if (response != null && response.getCode() == 200) {
            ActionResult ar = ActionResult.toActionResult(response.getBodyString());
            if (ar.getCode() == 999) {
                Object data = ar.getData();
                if (Boolean.TRUE.equals(data)) {
                    this.upateState(UPDATESTATE_SYNCABORT, taskId, new String[] { tableName }, null, null, DateUtil.getSysDate(), DateUtil.getSysTime());
                    return "停止成功";
                } else {
                    throw new BusinessException("Agent 返回数据异常");
                }
            } else {
                throw new BusinessException(ar.getMessage());
            }
        } else {
            throw new BusinessException("Agent 链接异常");
        }
    }

    public Boolean hasBeenRun(Long taskId) {
        String sql = "select count(tcji.*) from table_info ti " + "inner join table_cdc_job_info tcji on ti.table_id = tcji.table_id " + "where ti.database_id = ?";
        long number = Dbo.queryNumber(sql, taskId).orElse(0);
        return number > 0;
    }
}
