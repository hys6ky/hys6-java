package hyren.serv6.b.realtimecollection.sdmdataconsume.sdmdataoutput;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;
import static hyren.serv6.base.user.UserUtil.getUserId;

@DocClass(desc = "", author = "yec", createdate = "2021-04-28")
@Slf4j
@Service
public class SdmDataOutputService {

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_sp_output", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_textfile", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_database", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_stream", desc = "", range = "", isBean = true)
    @Param(name = "streampro_setting", desc = "", range = "", isBean = true)
    @Param(name = "sdm_rest_database", desc = "", range = "", isBean = true)
    @Param(name = "sdm_rest_stream", desc = "", range = "", isBean = true)
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "rest_key", desc = "", range = "", nullable = true)
    @Param(name = "rest_val", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Map<String, Object> saveDataOutputMsg(SdmSpOutput sdm_sp_output, SdmSpTextfile sdm_sp_textfile, SdmSpDatabase sdm_sp_database, SdmSpStream sdm_sp_stream, StreamproSetting streampro_setting, long ssj_job_id, long dsl_id, SdmRestDatabase sdm_rest_database, SdmRestStream sdm_rest_stream, List<String> rest_key, List<String> rest_val) {
        Map<String, Object> mapRest = new HashMap<>();
        long sdm_info_id = PrimayKeyGener.getNextId();
        long ssd_info_id = PrimayKeyGener.getNextId();
        sdm_sp_output.setSdm_info_id(sdm_info_id);
        sdm_sp_output.setSsj_job_id(ssj_job_id);
        Map<String, Object> mapNum = Dbo.queryOneObject("select Max(output_number) as max_num from " + SdmSpOutput.TableName);
        if (null == mapNum.get("max_num")) {
            sdm_sp_output.setOutput_number(1L);
        } else {
            sdm_sp_output.setOutput_number(Long.parseLong(mapNum.get("max_num").toString()) + 1L);
        }
        sdm_sp_output.add(Dbo.db());
        if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(sdm_sp_output.getOutput_type())) {
            sdm_sp_textfile.setSdm_info_id(sdm_info_id);
            sdm_sp_textfile.setTsst_extfile_id(PrimayKeyGener.getNextId());
            sdm_sp_textfile.add(Dbo.db());
        } else if (SdmSpInputOutputType.XIAOFEIZHUTI == SdmSpInputOutputType.ofEnumByCode(sdm_sp_output.getOutput_type())) {
            sdm_sp_stream.setSss_stream_id(PrimayKeyGener.getNextId());
            sdm_sp_stream.setSdm_info_id(sdm_info_id);
            sdm_sp_stream.setSss_consumer_offset("");
            sdm_sp_stream.add(Dbo.db());
        } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(sdm_sp_output.getOutput_type())) {
            long table_id = PrimayKeyGener.getNextId();
            sdm_sp_database.setSsd_info_id(ssd_info_id);
            sdm_sp_database.setCn_table_name(sdm_sp_output.getOutput_table_name());
            sdm_sp_database.setTab_id(table_id);
            sdm_sp_database.setSdm_info_id(sdm_info_id);
            sdm_sp_database.setDsl_id(dsl_id);
            sdm_sp_database.add(Dbo.db());
            DtabRelationStore relationTable = new DtabRelationStore();
            relationTable.setTab_id(table_id);
            relationTable.setDsl_id(dsl_id);
            relationTable.setIs_successful(JobExecuteState.WanCheng.getCode());
            relationTable.setData_source(StoreLayerDataSource.SD.getCode());
            relationTable.add(Dbo.db());
            DqTableInfo dq_table_info = new DqTableInfo();
            dq_table_info.setTable_space("");
            dq_table_info.setTable_id(table_id);
            dq_table_info.setIs_trace(IsFlag.Fou.getCode());
            dq_table_info.setCreate_id(getUserId());
            dq_table_info.setTable_name(sdm_sp_database.getSsd_table_name());
            dq_table_info.setCreate_date(DateUtil.getSysDate());
            dq_table_info.setEnd_date(Constant._MAX_DATE_8);
            dq_table_info.add(Dbo.db());
        } else if (SdmSpInputOutputType.REST == SdmSpInputOutputType.ofEnumByCode(sdm_sp_output.getOutput_type())) {
            streampro_setting.setRs_id(PrimayKeyGener.getNextId());
            streampro_setting.setSdm_info_id(sdm_info_id);
            Map<String, String> map = new HashMap<>();
            if (null != rest_key && rest_key.size() > 0) {
                for (int i = 0; i < rest_key.size(); i++) {
                    map.put(rest_key.get(i), rest_val.get(i));
                }
            }
            streampro_setting.setRs_para(JsonUtil.toJson(map));
            streampro_setting.add(Dbo.db());
            if (SdmSpRsType.ShuJuKu == SdmSpRsType.ofEnumByCode(streampro_setting.getRs_processing())) {
                sdm_rest_database.setRs_id(streampro_setting.getRs_id());
                sdm_rest_database.setSsd_info_id(PrimayKeyGener.getNextId());
                sdm_rest_database.add(Dbo.db());
            } else if (SdmSpRsType.KafKa == SdmSpRsType.ofEnumByCode(streampro_setting.getRs_processing())) {
                sdm_rest_stream.setSss_stream_id(PrimayKeyGener.getNextId());
                sdm_rest_stream.setRs_id(streampro_setting.getRs_id());
                sdm_rest_stream.setSss_consumer_offset("");
                sdm_rest_stream.add(Dbo.db());
            }
        }
        mapRest.put("sdm_info_id", sdm_info_id);
        mapRest.put("ssd_info_id", ssd_info_id);
        return mapRest;
    }

    @Method(desc = "", logicStep = "")
    public List<StreamproSetting> getRestList() {
        List<StreamproSetting> streamInfoList = Dbo.queryList(StreamproSetting.class, "select rs_id,rs_para,rs_processing,rs_type,rs_url from " + StreamproSetting.TableName);
        return streamInfoList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "rs_id", desc = "", range = "")
    public Map<String, Object> getCheckedMsg(long rs_id) {
        Map<String, Object> map = new HashMap<>();
        StreamproSetting settingInfo = Dbo.queryOneObject(StreamproSetting.class, "select rs_id,rs_para,rs_processing,rs_type,rs_url from " + StreamproSetting.TableName + " where rs_id=?", rs_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        map.put("settingInfo", settingInfo);
        if (SdmSpRsType.ShuJuKu == SdmSpRsType.ofEnumByCode(settingInfo.getRs_processing())) {
            SdmRestDatabase dataInfo = Dbo.queryOneObject(SdmRestDatabase.class, "select * from " + SdmRestDatabase.TableName + " where rs_id=?", rs_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            map.put("dataInfo", dataInfo);
        } else if (SdmSpRsType.KafKa == SdmSpRsType.ofEnumByCode(settingInfo.getRs_processing())) {
            SdmRestStream streamInfo = Dbo.queryOneObject(SdmRestStream.class, "select * from " + SdmRestStream.TableName + " where rs_id=?", rs_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            map.put("streamInfo", streamInfo);
        }
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    public List<Object> getSdmSpOutputMsgList(long ssj_job_id) {
        List<SdmSpOutput> outputList = Dbo.queryList(SdmSpOutput.class, "select * from " + SdmSpOutput.TableName + " where ssj_job_id = ?", ssj_job_id);
        List<Object> outMsgList = new ArrayList<>();
        for (SdmSpOutput outputInfo : outputList) {
            if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(outputInfo.getOutput_type())) {
                List<Map<String, Object>> textList = Dbo.queryList("SELECT t1.sdm_info_id,t1.output_type,t1.output_mode," + "t1.output_table_name,t2.sst_file_path as table_name" + " FROM sdm_sp_output t1 LEFT JOIN sdm_sp_textfile t2 ON t1.sdm_info_id = t2.sdm_info_id " + " where t1.sdm_info_id = ?", outputInfo.getSdm_info_id());
                outMsgList.add(textList);
            } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(outputInfo.getOutput_type())) {
                List<Map<String, Object>> dataList = Dbo.queryList("SELECT t1.sdm_info_id,t2.ssd_info_id,t1.output_type,t1.output_mode," + "t1.output_table_name,t2.ssd_table_name as table_name" + " FROM sdm_sp_output t1 LEFT JOIN sdm_sp_database t2 ON t1.sdm_info_id = t2.sdm_info_id " + " where t1.sdm_info_id = ?", outputInfo.getSdm_info_id());
                outMsgList.add(dataList);
            } else if (SdmSpInputOutputType.XIAOFEIZHUTI == SdmSpInputOutputType.ofEnumByCode(outputInfo.getOutput_type())) {
                List<Map<String, Object>> streamList = Dbo.queryList("SELECT t1.sdm_info_id,t1.output_type,t1.output_mode," + "t1.output_table_name,t2.sss_topic_name as table_name " + " FROM sdm_sp_output t1 LEFT JOIN sdm_sp_stream t2 ON t1.sdm_info_id = t2.sdm_info_id " + " where t1.sdm_info_id = ?", outputInfo.getSdm_info_id());
                outMsgList.add(streamList);
            } else if (SdmSpInputOutputType.REST == SdmSpInputOutputType.ofEnumByCode(outputInfo.getOutput_type())) {
                Optional<StreamproSetting> streampro_setting = Dbo.queryOneObject(StreamproSetting.class, "select * from " + StreamproSetting.TableName + " where sdm_info_id = ?", outputInfo.getSdm_info_id());
                if (streampro_setting.isPresent()) {
                    if (SdmSpRsType.HuLue == SdmSpRsType.ofEnumByCode(streampro_setting.get().getRs_processing())) {
                        List<Map<String, Object>> restList = Dbo.queryList("SELECT t1.sdm_info_id, t1.output_type, t1.output_mode, t1.output_table_name " + " FROM sdm_sp_output t1 JOIN streampro_setting t2 ON t1.sdm_info_id = t2.sdm_info_id " + " WHERE t1.sdm_info_id = ?", outputInfo.getSdm_info_id());
                        outMsgList.add(restList);
                    } else {
                        List<Map<String, Object>> restList = Dbo.queryList("SELECT t1.sdm_info_id, t1.output_type, t1.output_mode, t1.output_table_name, t3.sss_topic_name AS TABLE_NAME " + " FROM sdm_sp_output t1 JOIN streampro_setting t2 ON t1.sdm_info_id = t2.sdm_info_id " + " JOIN sdm_rest_stream t3 on t2.rs_id=t3.rs_id " + " WHERE t1.sdm_info_id = ?", outputInfo.getSdm_info_id());
                        outMsgList.add(restList);
                    }
                }
            }
        }
        return outMsgList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_info_id", desc = "", range = "")
    public Map<String, Object> getSdmSpOutputMsg(long sdm_info_id) {
        Map<String, Object> map = new HashMap<>();
        SdmSpOutput spInfo = Dbo.queryOneObject(SdmSpOutput.class, "select * from " + SdmSpOutput.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        map.put("spInfo", spInfo);
        if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            SdmSpTextfile textInfo = Dbo.queryOneObject(SdmSpTextfile.class, "select * from " + SdmSpTextfile.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            map.put("textInfo", textInfo);
        } else if (SdmSpInputOutputType.XIAOFEIZHUTI == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            SdmSpStream streamInfo = Dbo.queryOneObject(SdmSpStream.class, "select * from " + SdmSpStream.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            map.put("streamInfo", streamInfo);
        } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            SdmSpDatabase dataInfo = Dbo.queryOneObject(SdmSpDatabase.class, "select * from " + SdmSpDatabase.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            map.put("dataInfo", dataInfo);
        } else if (SdmSpInputOutputType.REST == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            StreamproSetting proInfo = Dbo.queryOneObject(StreamproSetting.class, "select * from " + StreamproSetting.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            map.put("proInfo", proInfo);
            if (!StringUtil.isEmpty(proInfo.getRs_processing())) {
                if (SdmSpRsType.ShuJuKu == SdmSpRsType.ofEnumByCode(proInfo.getRs_processing())) {
                    SdmRestDatabase dataInfo = Dbo.queryOneObject(SdmRestDatabase.class, "select * from " + SdmRestDatabase.TableName + " where rs_id = ?", proInfo.getRs_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                    map.put("dataInfo", dataInfo);
                } else if (SdmSpRsType.KafKa == SdmSpRsType.ofEnumByCode(proInfo.getRs_processing())) {
                    SdmRestStream streamInfo = Dbo.queryOneObject(SdmRestStream.class, "select * from " + SdmRestStream.TableName + " where rs_id = ?", proInfo.getRs_id()).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
                    map.put("streamInfo", streamInfo);
                }
            }
        }
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_sp_output", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_textfile", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_database", desc = "", range = "", isBean = true)
    @Param(name = "sdm_sp_stream", desc = "", range = "", isBean = true)
    @Param(name = "streampro_setting", desc = "", range = "", isBean = true)
    @Param(name = "sdm_rest_database", desc = "", range = "", isBean = true)
    @Param(name = "sdm_rest_stream", desc = "", range = "", isBean = true)
    @Param(name = "sdm_info_id", desc = "", range = "")
    @Param(name = "rest_key", desc = "", range = "", nullable = true)
    @Param(name = "rest_val", desc = "", range = "", nullable = true)
    public void updateSdmSpOutputMsg(SdmSpOutput sdm_sp_output, SdmSpTextfile sdm_sp_textfile, SdmSpDatabase sdm_sp_database, SdmSpStream sdm_sp_stream, StreamproSetting streampro_setting, long sdm_info_id, SdmRestDatabase sdm_rest_database, SdmRestStream sdm_rest_stream, List<String> rest_key, List<String> rest_val) {
        SdmSpOutput spInfo = Dbo.queryOneObject(SdmSpOutput.class, "select * from " + SdmSpOutput.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        sdm_sp_output.setSdm_info_id(sdm_info_id);
        sdm_sp_output.update(Dbo.db());
        if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            SdmSpTextfile textfile = Dbo.queryOneObject(SdmSpTextfile.class, "select * from " + SdmSpTextfile.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            sdm_sp_textfile.setTsst_extfile_id(textfile.getTsst_extfile_id());
            sdm_sp_textfile.update(Dbo.db());
        } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            SdmSpDatabase baseInfo = Dbo.queryOneObject(SdmSpDatabase.class, "select * from " + SdmSpDatabase.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            sdm_sp_database.setSsd_info_id(baseInfo.getSsd_info_id());
            sdm_sp_database.update(Dbo.db());
        } else if (SdmSpInputOutputType.XIAOFEIZHUTI == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            SdmSpStream streamInfo = Dbo.queryOneObject(SdmSpStream.class, "select * from " + SdmSpStream.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            sdm_sp_stream.setSss_stream_id(streamInfo.getSss_stream_id());
            sdm_sp_stream.setSss_consumer_offset("");
            sdm_sp_stream.update(Dbo.db());
        } else if (SdmSpInputOutputType.REST == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            StreamproSetting settingInfo = Dbo.queryOneObject(StreamproSetting.class, "select * from " + StreamproSetting.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            streampro_setting.setRs_id(settingInfo.getRs_id());
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < rest_key.size(); i++) {
                map.put(rest_key.get(i), rest_val.get(i));
            }
            streampro_setting.setRs_para(JsonUtil.toJson(map));
            streampro_setting.update(Dbo.db());
            if (SdmSpRsType.ShuJuKu == SdmSpRsType.ofEnumByCode(streampro_setting.getRs_processing())) {
                Dbo.execute("delete from " + SdmRestDatabase.TableName + " where rs_id = ?", settingInfo.getRs_id());
                sdm_rest_database.setRs_id(streampro_setting.getRs_id());
                sdm_rest_database.setSsd_info_id(PrimayKeyGener.getNextId());
                sdm_rest_database.add(Dbo.db());
            } else if (SdmSpRsType.KafKa == SdmSpRsType.ofEnumByCode(streampro_setting.getRs_processing())) {
                Dbo.execute("delete from " + SdmRestStream.TableName + " where rs_id = ?", settingInfo.getRs_id());
                sdm_rest_stream.setSss_stream_id(PrimayKeyGener.getNextId());
                sdm_rest_stream.setRs_id(streampro_setting.getRs_id());
                sdm_rest_stream.setSss_consumer_offset("");
                sdm_rest_stream.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean testOutPutConnection(long dsl_id) {
        DatabaseWrapper db = ConnectionTool.getDBWrapper(Dbo.db(), dsl_id);
        return db.isConnected();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "url", desc = "", range = "")
    @Param(name = "rest_key", desc = "", range = "", nullable = true)
    @Param(name = "rest_val", desc = "", range = "", nullable = true)
    public void testMethodConnection(String url, String[] rest_key, String[] rest_val) {
        Map<String, String> map = new HashMap<>();
        if (null != rest_key && rest_key.length > 0) {
            for (int i = 0; i < rest_key.length; i++) {
                map.put(rest_key[i], rest_val[i]);
            }
            for (Map.Entry<String, String> entry : map.entrySet()) {
                HttpClient.ResponseValue resVal = new HttpClient().addData(entry.getKey(), entry.getValue()).post(url);
                ActionResult actionResult = ActionResult.toActionResult(resVal.getBodyString());
                if (!actionResult.isSuccess()) {
                    throw new BusinessException("接口调用失败!");
                }
            }
        } else {
            HttpClient.ResponseValue resVal = new HttpClient().post(url);
            ActionResult actionResult = ActionResult.toActionResult(resVal.getBodyString());
            if (!actionResult.isSuccess()) {
                throw new BusinessException("接口调用失败!");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sdm_info_id", desc = "", range = "")
    @Param(name = "ssd_info_id", desc = "", range = "")
    public void deleteSdmSpOutputMsg(long sdm_info_id, long ssd_info_id) {
        SdmSpOutput spInfo = Dbo.queryOneObject(SdmSpOutput.class, "select * from " + SdmSpOutput.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        if (SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            Dbo.execute("delete from " + SdmSpTextfile.TableName + " where sdm_info_id = ?", sdm_info_id);
        } else if (SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            Optional<SdmSpDatabase> sp_baseInfo = Dbo.queryOneObject(SdmSpDatabase.class, "select * from " + SdmSpDatabase.TableName + " where ssd_info_id = ?", ssd_info_id);
            if (sp_baseInfo.isPresent()) {
                Dbo.execute("delete from " + DtabRelationStore.TableName + " where tab_id = ?", sp_baseInfo.get().getTab_id());
                Dbo.execute("delete from " + SdmSpDatabase.TableName + " where sdm_info_id = ?", sdm_info_id);
            }
        } else if (SdmSpInputOutputType.XIAOFEIZHUTI == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            Dbo.execute("delete from " + SdmSpStream.TableName + " where sdm_info_id = ?", sdm_info_id);
        } else if (SdmSpInputOutputType.REST == SdmSpInputOutputType.ofEnumByCode(spInfo.getOutput_type())) {
            StreamproSetting settingInfo = Dbo.queryOneObject(StreamproSetting.class, "select * from " + StreamproSetting.TableName + " where sdm_info_id = ?", sdm_info_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
            if (SdmSpRsType.ShuJuKu == SdmSpRsType.ofEnumByCode(settingInfo.getRs_processing())) {
                Dbo.execute("delete from " + SdmRestDatabase.TableName + " where rs_id = ?", settingInfo.getRs_id());
            } else if (SdmSpRsType.KafKa == SdmSpRsType.ofEnumByCode(settingInfo.getRs_processing())) {
                Dbo.execute("delete from " + SdmRestStream.TableName + " where rs_id = ?", settingInfo.getRs_id());
            }
            Dbo.execute("delete from " + StreamproSetting.TableName + " where sdm_info_id = ?", sdm_info_id);
        }
        Dbo.execute("delete from " + SdmSpOutput.TableName + " where sdm_info_id = ?", sdm_info_id);
    }
}
