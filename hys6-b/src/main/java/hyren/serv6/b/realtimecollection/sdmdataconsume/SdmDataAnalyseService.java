package hyren.serv6.b.realtimecollection.sdmdataconsume;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.SdmSpInputOutputType;
import hyren.serv6.base.codes.SdmSpStreamVer;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.JDBCBean;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.stream.KafkaMonitorManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static hyren.serv6.base.user.UserUtil.getUser;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2021-04-23")
@Slf4j
@Service
public class SdmDataAnalyseService {

    @Autowired
    KafkaMonitorManager manager;

    @Method(desc = "", logicStep = "")
    @Param(name = "sdmSpInfo", desc = "", range = "", isBean = true)
    public long saveTask(SdmSpJobinfo sdmSpInfo) {
        if (Dbo.queryNumber("select count(1) from " + SdmSpJobinfo.TableName + " where ssj_job_name = ? ", sdmSpInfo.getSsj_job_name()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("该任务已存在，请勿重复添加!");
        }
        sdmSpInfo.setSsj_job_id(PrimayKeyGener.getNextId());
        sdmSpInfo.setUser_id(getUserId());
        sdmSpInfo.add(Dbo.db());
        return sdmSpInfo.getSsj_job_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    public void deleteTask(long ssj_job_id) {
        List<SdmJobInput> jobList = Dbo.queryList(SdmJobInput.class, "select * from " + SdmJobInput.TableName + " where ssj_job_id = ?", ssj_job_id);
        if (!jobList.isEmpty()) {
            for (SdmJobInput jobInfo : jobList) {
                if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(jobInfo.getInput_type())) {
                    Dbo.execute("delete from " + SdmSpTextfile.TableName + " where sdm_info_id = ?", jobInfo.getSdm_info_id());
                } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(jobInfo.getInput_type())) {
                    Dbo.execute("delete from " + SdmInputDatabase.TableName + " where sdm_info_id = ?", jobInfo.getSdm_info_id());
                } else {
                    Dbo.execute("delete from " + SdmSpStream.TableName + " where sdm_info_id = ?", jobInfo.getSdm_info_id());
                }
            }
            Dbo.execute("delete from " + SdmJobInput.TableName + " where ssj_job_id = ?", ssj_job_id);
        }
        Result analyResult = Dbo.queryResult("select * from " + SdmSpAnalysis.TableName + " where ssj_job_id = ?", ssj_job_id);
        if (!analyResult.isEmpty()) {
            Dbo.execute("delete from " + SdmSpAnalysis.TableName + " where ssj_job_id = ?", ssj_job_id);
        }
        List<SdmSpOutput> outputInfoList = Dbo.queryList(SdmSpOutput.class, "select * from " + SdmSpOutput.TableName + " where ssj_job_id = ?", ssj_job_id);
        if (!outputInfoList.isEmpty()) {
            for (SdmSpOutput outputInfo : outputInfoList) {
                if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(outputInfo.getOutput_type())) {
                    Dbo.execute("delete from " + SdmSpTextfile.TableName + " where sdm_info_id = ?", outputInfo.getSdm_info_id());
                } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(outputInfo.getOutput_type())) {
                    Dbo.execute("delete from " + SdmSpDatabase.TableName + " where sdm_info_id = ?", outputInfo.getSdm_info_id());
                } else {
                    Dbo.execute("delete from " + SdmSpStream.TableName + " where sdm_info_id = ?", outputInfo.getSdm_info_id());
                }
            }
            Dbo.execute("delete from " + SdmSpOutput.TableName + " where ssj_job_id = ?", ssj_job_id);
        }
        Dbo.execute("delete from " + SdmSpJobinfo.TableName + " where ssj_job_id = ?", ssj_job_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> selectTaskList(int currPage, int pageSize) {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + SdmSpJobinfo.TableName + " where user_id = ?  order by ssj_job_id desc");
        asmSql.addParam(getUserId());
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<SdmSpJobinfo> jobInfoList = Dbo.queryPagedList(SdmSpJobinfo.class, page, asmSql.sql(), asmSql.params());
        String ssp_param_value = "";
        List<Object> parList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        for (SdmSpJobinfo jobInfo : jobInfoList) {
            List<SdmSpParam> paramList = Dbo.queryList(SdmSpParam.class, "select * from " + SdmSpParam.TableName + " where ssj_job_id = ?", jobInfo.getSsj_job_id());
            for (SdmSpParam param : paramList) {
                if ("streaming_duration".equals(param.getSsp_param_key())) {
                    ssp_param_value = param.getSsp_param_value();
                }
            }
            Map<String, Object> jsonObject = JsonUtil.toObject(JsonUtil.toJson(jobInfo), new TypeReference<Map<String, Object>>() {
            });
            jsonObject.put("streaming_duration", ssp_param_value);
            parList.add(jsonObject);
        }
        map.put("paramInfo", parList);
        map.put("totalSize", page.getTotalSize());
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "ssj_job_name", desc = "", range = "")
    @Param(name = "ssj_strategy", desc = "", range = "")
    @Param(name = "ssj_job_desc", desc = "", range = "", nullable = true)
    public void updateTask(long ssj_job_id, String ssj_job_name, String ssj_strategy, String ssj_job_desc) {
        SdmSpJobinfo spInfo = Dbo.queryOneObject(SdmSpJobinfo.class, "select * from " + SdmSpJobinfo.TableName + " where ssj_job_id=?", ssj_job_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        if (ssj_job_name.equals(spInfo.getSsj_job_name())) {
            Dbo.execute("update " + SdmSpJobinfo.TableName + " set ssj_strategy = ?, ssj_job_desc = ?" + " where ssj_job_id=?", ssj_strategy, ssj_job_desc, ssj_job_id);
        } else {
            if (Dbo.queryNumber("SELECT count(1) FROM " + SdmSpJobinfo.TableName + " WHERE ssj_job_name=?", ssj_job_name).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
                throw new BusinessException("任务名重复!");
            }
            Dbo.execute("update " + SdmSpJobinfo.TableName + " set ssj_job_name = ?,ssj_strategy = ?, ssj_job_desc = ?" + " where ssj_job_id=?", ssj_job_name, ssj_strategy, ssj_job_desc, ssj_job_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_job_input", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_textfile", desc = "", range = "", isBean = true)
    @Param(name = "sdm_input_database", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_stream", desc = "", range = "", isBean = true)
    public void saveJobSource(SdmJobInput sdm_job_input, SdmSpTextfile sdm_sp_textfile, SdmInputDatabase sdm_input_database, SdmSpStream sdm_sp_stream) {
        if (Dbo.queryNumber("select count(1) from " + SdmJobInput.TableName + " where ssj_job_id = ? and input_table_name = ?", sdm_job_input.getSsj_job_id(), sdm_job_input.getInput_table_name()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("映射表不能重复!");
        }
        Map<String, Object> mapNum = Dbo.queryOneObject("select Max(input_number) as max_num from " + SdmJobInput.TableName);
        sdm_job_input.setSdm_info_id(PrimayKeyGener.getNextId());
        if (null == mapNum.get("max_num")) {
            sdm_job_input.setInput_number(1L);
        } else {
            sdm_job_input.setInput_number(Long.parseLong(mapNum.get("max_num").toString()) + 1L);
        }
        if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(sdm_job_input.getInput_type())) {
            sdm_job_input.setInput_en_name(sdm_sp_textfile.getSst_file_path());
            sdm_job_input.add(Dbo.db());
            sdm_sp_textfile.setTsst_extfile_id(PrimayKeyGener.getNextId());
            sdm_sp_textfile.setSdm_info_id(sdm_job_input.getSdm_info_id());
            sdm_sp_textfile.add(Dbo.db());
        } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(sdm_job_input.getInput_type())) {
            sdm_job_input.setInput_en_name(sdm_input_database.getSsd_table_name());
            sdm_job_input.add(Dbo.db());
            sdm_input_database.setSsd_info_id(PrimayKeyGener.getNextId());
            sdm_input_database.setSdm_info_id(sdm_job_input.getSdm_info_id());
            sdm_input_database.add(Dbo.db());
        } else if (SdmSpInputOutputType.XIAOFEIZHUTI == SdmSpInputOutputType.ofEnumByCode(sdm_job_input.getInput_type())) {
            sdm_job_input.setInput_en_name(sdm_sp_stream.getSss_topic_name());
            sdm_job_input.add(Dbo.db());
            sdm_sp_stream.setSss_stream_id(PrimayKeyGener.getNextId());
            sdm_sp_stream.setSdm_info_id(sdm_job_input.getSdm_info_id());
            sdm_sp_stream.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_job_input", desc = "", range = "", isBean = true)
    @Param(name = "outputTableName", desc = "", range = "")
    @Param(name = "sss_consumer_offset", desc = "", range = "")
    public void saveJobInteriorSource(SdmJobInput sdm_job_input, String outputTableName, String sss_consumer_offset) {
        if (Dbo.queryNumber("select count(1) from " + SdmJobInput.TableName + " where ssj_job_id = ? and input_table_name = ?", sdm_job_input.getSsj_job_id(), outputTableName).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("映射表不能重复!");
        }
        Map<String, Object> mapNum = Dbo.queryOneObject("select Max(input_number) as max_num from " + SdmJobInput.TableName);
        sdm_job_input.setSdm_info_id(PrimayKeyGener.getNextId());
        if (null == mapNum.get("max_num")) {
            sdm_job_input.setInput_number(1L);
        } else {
            sdm_job_input.setInput_number(Long.parseLong(mapNum.get("max_num").toString()) + 1L);
        }
        sdm_job_input.setInput_table_name(outputTableName);
        sdm_job_input.add(Dbo.db());
        SdmSpStream spStream = new SdmSpStream();
        String brokerServer = manager.parseBrokerServer();
        spStream.setSss_stream_id(PrimayKeyGener.getNextId());
        spStream.setSdm_info_id(sdm_job_input.getSdm_info_id());
        spStream.setSss_bootstrap_server(brokerServer);
        spStream.setSss_consumer_offset(sss_consumer_offset);
        spStream.setSss_kafka_version(SdmSpStreamVer.KAFKA.getCode());
        spStream.setSss_topic_name(sdm_job_input.getInput_en_name());
        spStream.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_info_id", desc = "", range = "")
    @Param(name = "is_inner", desc = "", range = "")
    public void deleteSourceInfo(long sdm_info_id, String is_inner) {
        SdmJobInput jobInfo = Dbo.queryOneObject(SdmJobInput.class, "select * from " + SdmJobInput.TableName + " where sdm_info_id=?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        DboExecute.deletesOrThrow("根据sdm_info_id删除sdm_job_input信息表数据失败，sdm_info_id = " + sdm_info_id, "delete from " + SdmJobInput.TableName + " where sdm_info_id = ? ", sdm_info_id);
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            if (IsFlag.Fou == IsFlag.ofEnumByCode(is_inner)) {
                SqlOperator.execute(db, "delete from " + SdmSpStream.TableName + " where sdm_info_id = ? ", sdm_info_id);
            } else if (IsFlag.Shi == IsFlag.ofEnumByCode(is_inner)) {
                if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(jobInfo.getInput_type())) {
                    SqlOperator.execute(db, "delete from " + SdmSpTextfile.TableName + " where sdm_info_id = ? ", sdm_info_id);
                } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(jobInfo.getInput_type())) {
                    SqlOperator.execute(db, "delete from " + SdmInputDatabase.TableName + " where sdm_info_id = ? ", sdm_info_id);
                } else if (SdmSpInputOutputType.XIAOFEIZHUTI == SdmSpInputOutputType.ofEnumByCode(jobInfo.getInput_type())) {
                    SqlOperator.execute(db, "delete from " + SdmSpStream.TableName + " where sdm_info_id = ? ", sdm_info_id);
                }
            }
            db.commit();
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_info_id", desc = "", range = "")
    @Param(name = "is_inner", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getSourceInfo(long sdm_info_id, String is_inner) {
        Map<String, Object> map = new HashMap<>();
        SdmJobInput jobInfo = Dbo.queryOneObject(SdmJobInput.class, "select * from " + SdmJobInput.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        map.put("sp_jobInfo", jobInfo);
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_inner)) {
            SdmSpStream streamInfo = Dbo.queryOneObject(SdmSpStream.class, "select * from " + SdmSpStream.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            map.put("sp_streamInfo", streamInfo);
        } else if (IsFlag.Shi == IsFlag.ofEnumByCode(is_inner)) {
            if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(jobInfo.getInput_type())) {
                SdmSpTextfile textInfo = Dbo.queryOneObject(SdmSpTextfile.class, "select * from " + SdmSpTextfile.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                map.put("sp_textInfo", textInfo);
            } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(jobInfo.getInput_type())) {
                SdmInputDatabase dataInfo = Dbo.queryOneObject(SdmInputDatabase.class, "select * from " + SdmInputDatabase.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                map.put("sp_dataInfo", dataInfo);
            } else {
                SdmSpStream streamInfo = Dbo.queryOneObject(SdmSpStream.class, "select * from " + SdmSpStream.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                map.put("sp_streamInfo", streamInfo);
            }
        }
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_job_input", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_textfile", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_stream", desc = "", range = "", isBean = true)
    @Param(name = "sdm_input_database", desc = "", range = "", isBean = true)
    @Param(name = "is_inner", desc = "", range = "")
    public void updateSourceInfo(SdmJobInput sdm_job_input, SdmSpTextfile sdm_sp_textfile, SdmSpStream sdm_sp_stream, SdmInputDatabase sdm_input_database, String is_inner) {
        if (Dbo.queryNumber("select count(1) from " + SdmJobInput.TableName + " where ssj_job_id = ? and input_table_name = ? and sdm_info_id !=?", sdm_job_input.getSsj_job_id(), sdm_job_input.getInput_table_name(), sdm_job_input.getSdm_info_id()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("映射表不能重复!");
        }
        if (IsFlag.Fou == IsFlag.ofEnumByCode(is_inner)) {
            SdmSpStream streamInfo = Dbo.queryOneObject(SdmSpStream.class, "select * from " + SdmSpStream.TableName + " where sdm_info_id = ?", sdm_job_input.getSdm_info_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            sdm_job_input.setInput_en_name(sdm_sp_stream.getSss_topic_name());
            sdm_job_input.update(Dbo.db());
            sdm_sp_stream.setSss_stream_id(streamInfo.getSss_stream_id());
            sdm_sp_stream.update(Dbo.db());
        } else if (IsFlag.Shi == IsFlag.ofEnumByCode(is_inner)) {
            if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(sdm_job_input.getInput_type())) {
                SdmSpTextfile fileInfo = Dbo.queryOneObject(SdmSpTextfile.class, "select * from " + SdmSpTextfile.TableName + " where sdm_info_id = ?", sdm_job_input.getSdm_info_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                sdm_job_input.setInput_en_name(sdm_sp_textfile.getSst_file_path());
                sdm_job_input.update(Dbo.db());
                sdm_sp_textfile.setTsst_extfile_id(fileInfo.getTsst_extfile_id());
                sdm_sp_textfile.update(Dbo.db());
            } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(sdm_job_input.getInput_type())) {
                SdmInputDatabase dataInfo = Dbo.queryOneObject(SdmInputDatabase.class, "select * from " + SdmInputDatabase.TableName + " where sdm_info_id = ?", sdm_job_input.getSdm_info_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                sdm_job_input.setInput_en_name(sdm_input_database.getSsd_table_name());
                sdm_job_input.update(Dbo.db());
                sdm_input_database.setSsd_info_id(dataInfo.getSsd_info_id());
                sdm_input_database.update(Dbo.db());
            } else {
                SdmSpStream streamInfo = Dbo.queryOneObject(SdmSpStream.class, "select * from " + SdmSpStream.TableName + " where sdm_info_id = ?", sdm_job_input.getSdm_info_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                sdm_job_input.setInput_en_name(sdm_sp_stream.getSss_topic_name());
                sdm_job_input.update(Dbo.db());
                sdm_sp_stream.setSss_stream_id(streamInfo.getSss_stream_id());
                sdm_sp_stream.update(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_input_database", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public boolean testConnection(SdmInputDatabase sdm_input_database) {
        JDBCBean bean = new JDBCBean();
        bean.setDatabase_name(sdm_input_database.getSsd_database_drive());
        bean.setJdbc_url(sdm_input_database.getSsd_jdbc_url());
        bean.setUser_name(sdm_input_database.getSsd_user_name());
        bean.setDatabase_pad(sdm_input_database.getSsd_user_password());
        bean.setDatabase_type(sdm_input_database.getSsd_database_type());
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(bean)) {
            return db.isConnected();
        } catch (Exception e) {
            throw new BusinessException("数据库链接失败!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<SdmJobInput> getDataSourceList(long ssj_job_id) {
        return Dbo.queryList(SdmJobInput.class, "select sdm_info_id,input_en_name,input_table_name,input_source,input_data_type from " + SdmJobInput.TableName + "  where ssj_job_id = ? order by input_number asc", ssj_job_id);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Node> geKFKTreeData() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.TRUE);
        DataSourceType[] dataSourceTypes = { DataSourceType.DCL, DataSourceType.DML, DataSourceType.DQC, DataSourceType.UDL };
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(dataSourceTypes, getUser(), treeConf);
        Map<String, Object> map = new HashMap<>();
        map.put("id", DataSourceType.KFK.getCode());
        map.put("label", DataSourceType.KFK.getValue());
        map.put("parent_id", "0");
        map.put("description", DataSourceType.KFK.getValue());
        dataList.add(map);
        List<DataSource> sdm_data_sourceList = getKFKDataSource();
        if (!sdm_data_sourceList.isEmpty()) {
            dataList.addAll(conversionKFKDataInfos(sdm_data_sourceList));
            sdm_data_sourceList.forEach(data_source -> {
                List<AgentInfo> kfkAgentTableInfoList = getKFKAgent(data_source);
                if (!kfkAgentTableInfoList.isEmpty()) {
                    dataList.addAll(conversionKFKTableInfos(kfkAgentTableInfoList));
                }
                kfkAgentTableInfoList.forEach(agent_info -> {
                    List<Map<String, Object>> kfkTaskTableInfoList = getKFKTask(agent_info);
                    if (!kfkTaskTableInfoList.isEmpty()) {
                        dataList.addAll(conversionKFKTaskInfos(kfkTaskTableInfoList));
                    }
                });
            });
        }
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<DataSource> getKFKDataSource() {
        return Dbo.queryList(DataSource.class, "SELECT source_id,datasource_name,datasource_number,source_remark " + " FROM " + DataSource.TableName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_data_sourceList", desc = "", range = "")
    public static List<Map<String, Object>> conversionKFKDataInfos(List<DataSource> sdm_data_sourceList) {
        List<Map<String, Object>> kfkDataNodes = new ArrayList<>();
        for (DataSource dataSource : sdm_data_sourceList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + dataSource.getSource_id());
            map.put("label", dataSource.getDatasource_name());
            map.put("parent_id", DataSourceType.KFK.getCode());
            map.put("data_source_id", dataSource.getSource_id());
            map.put("description", "" + "数据源编号：" + dataSource.getSource_id() + "\n" + "数据源名称：" + dataSource.getDatasource_name() + "\n" + "数据源描述：" + dataSource.getDatasource_remark());
            kfkDataNodes.add(map);
        }
        return kfkDataNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data_source", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<AgentInfo> getKFKAgent(DataSource data_source) {
        return Dbo.queryList(AgentInfo.class, "select * from " + AgentInfo.TableName + " where source_id = ?", data_source.getSource_id());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "kfkAgentTableInfoList", desc = "", range = "")
    public static List<Map<String, Object>> conversionKFKTableInfos(List<AgentInfo> kfkAgentTableInfoList) {
        List<Map<String, Object>> kfkAgentNodes = new ArrayList<>();
        for (AgentInfo agentInfo : kfkAgentTableInfoList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + agentInfo.getAgent_id());
            map.put("label", agentInfo.getAgent_name());
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + agentInfo.getSource_id());
            map.put("agent_id", agentInfo.getAgent_id());
            map.put("description", "Agent名称：" + agentInfo.getAgent_name());
            kfkAgentNodes.add(map);
        }
        return kfkAgentNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_info", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getKFKTask(AgentInfo agent_info) {
        return Dbo.queryList("select t1.sdm_receive_id,t1.sdm_agent_id,t1.sdm_receive_name, t2.sdm_param_value from " + " sdm_receive_conf t1 JOIN sdm_rec_param t2 ON t1.sdm_receive_id = t2.sdm_receive_id " + " WHERE t1.sdm_agent_id = ? and t2.sdm_param_key='topic'", agent_info.getAgent_id());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "kfkTaskTableInfoList", desc = "", range = "")
    public static List<Map<String, Object>> conversionKFKTaskInfos(List<Map<String, Object>> kfkTaskTableInfoList) {
        List<Map<String, Object>> kfkTaskNodes = new ArrayList<>();
        for (Map<String, Object> taskInfo : kfkTaskTableInfoList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", DataSourceType.KFK.getCode() + "_" + taskInfo.get("sdm_receive_id"));
            map.put("label", taskInfo.get("sdm_receive_name"));
            map.put("parent_id", DataSourceType.KFK.getCode() + "_" + taskInfo.get("sdm_agent_id"));
            map.put("file_id", taskInfo.get("sdm_receive_id"));
            map.put("original_name", taskInfo.get("sdm_receive_name"));
            map.put("table_name", taskInfo.get("sdm_param_value"));
            map.put("description", "任务名称：" + taskInfo.get("sdm_receive_name"));
            kfkTaskNodes.add(map);
        }
        return kfkTaskNodes;
    }
}
