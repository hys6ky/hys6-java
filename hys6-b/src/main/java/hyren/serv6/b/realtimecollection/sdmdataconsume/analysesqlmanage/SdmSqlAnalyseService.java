package hyren.serv6.b.realtimecollection.sdmdataconsume.analysesqlmanage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.SdmSpInputOutputType;
import hyren.serv6.base.entity.SdmJobInput;
import hyren.serv6.base.entity.SdmSpAnalysis;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.stream.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "yec", createdate = "2021-04-28")
@Slf4j
@Service
public class SdmSqlAnalyseService {

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "analysis_table_name", desc = "", range = "")
    @Param(name = "analysis_sql", desc = "", range = "")
    public void saveAnalyseSql(long ssj_job_id, List<String> analysis_table_name, List<String> analysis_sql) {
        for (int i = 0; i < analysis_sql.size(); i++) {
            Result result = Dbo.queryResult("select input_table_name from " + SdmJobInput.TableName + " where ssj_job_id = ? and input_table_name = ?", ssj_job_id, analysis_table_name.get(i));
            if (!result.isEmpty()) {
                throw new BusinessException("请勿添加重复的数据!");
            }
            for (int j = i + 1; j < analysis_sql.size(); j++) {
                if (analysis_table_name.get(i).equals(analysis_table_name.get(j))) {
                    throw new BusinessException("输出表名请勿重复!");
                }
            }
        }
        Dbo.execute("delete from sdm_sp_analysis where ssj_job_id = ? ", ssj_job_id);
        SdmSpAnalysis analysisInfo = new SdmSpAnalysis();
        for (int i = 0; i < analysis_sql.size(); i++) {
            Map<String, Object> mapNum = Dbo.queryOneObject("select Max(analysis_number) as max_num from " + SdmSpAnalysis.TableName);
            if (null == mapNum.get("max_num")) {
                analysisInfo.setAnalysis_number(1L);
            } else {
                analysisInfo.setAnalysis_number(Long.parseLong(mapNum.get("max_num").toString()) + 1L);
            }
            analysisInfo.setSsa_info_id(PrimayKeyGener.getNextId());
            analysisInfo.setAnalysis_sql(StringUtil.string2Unicode(analysis_sql.get(i)));
            analysisInfo.setAnalysis_table_name(analysis_table_name.get(i));
            analysisInfo.setSsj_job_id(ssj_job_id);
            analysisInfo.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<SdmSpAnalysis> getAnalyseSql(long ssj_job_id) {
        List<SdmSpAnalysis> analysesList = Dbo.queryList(SdmSpAnalysis.class, "select * from " + SdmSpAnalysis.TableName + " where ssj_job_id = ? ", ssj_job_id);
        for (SdmSpAnalysis analyInfo : analysesList) {
            analyInfo.setAnalysis_sql(StringUtil.unicode2String(analyInfo.getAnalysis_sql()));
        }
        return analysesList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssa_info_id", desc = "", range = "")
    public void deleteAnalyseSql(long ssa_info_id) {
        DboExecute.deletesOrThrow("删除sdm_sp_analysis表信息失败!", "delete from " + SdmSpAnalysis.TableName + " where ssa_info_id = ?", ssa_info_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "pageStep", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getSdmAnalyseDataInfos(String pageStep) {
        List<Map<String, Object>> sdmDataInfos = new ArrayList<>();
        Map<String, Object> fileMap = new HashMap<>();
        fileMap.put("id", KafkaConstant.WENBEN_FILE);
        fileMap.put("label", "文本文件");
        fileMap.put("parent_id", "0");
        sdmDataInfos.add(fileMap);
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("id", KafkaConstant.DATA_BASE_TABLE);
        dataMap.put("label", "数据库表");
        dataMap.put("parent_id", "0");
        sdmDataInfos.add(dataMap);
        Map<String, Object> conMap = new HashMap<>();
        conMap.put("id", KafkaConstant.CONSUMER_TOPIC);
        conMap.put("label", "消费主题");
        conMap.put("parent_id", "0");
        sdmDataInfos.add(conMap);
        if ("3".equals(pageStep)) {
            Map<String, Object> sqlAnalyseMap = new HashMap<>();
            sqlAnalyseMap.put("id", KafkaConstant.ANALYSE_RESULT);
            sqlAnalyseMap.put("label", "分析结果表");
            sqlAnalyseMap.put("parent_id", "0");
            sdmDataInfos.add(sqlAnalyseMap);
        }
        return sdmDataInfos;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "ssj_job_id", desc = "", range = "")
    @Param(name = "pageStep", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getTableDataList(long ssj_job_id, String pageStep) {
        List<Map<String, Object>> dataList = new ArrayList<>();
        List<Map<String, Object>> tableList = Dbo.queryList("select sdm_info_id,input_type,input_table_name from " + SdmJobInput.TableName + " where ssj_job_id = ?", ssj_job_id);
        if (!tableList.isEmpty()) {
            dataList.addAll(conversionTableInfos(tableList));
        }
        if ("3".equals(pageStep)) {
            List<Map<String, Object>> analyseTableList = Dbo.queryList("select ssa_info_id,analysis_table_name from " + SdmSpAnalysis.TableName + " where ssj_job_id = ?", ssj_job_id);
            if (!analyseTableList.isEmpty()) {
                dataList.addAll(conversionAnalyseTableInfos(analyseTableList));
            }
        }
        return dataList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableList", desc = "", range = "")
    public static List<Map<String, Object>> conversionTableInfos(List<Map<String, Object>> tableList) {
        List<Map<String, Object>> tableMapNodes = new ArrayList<>();
        for (Map<String, Object> tableInfo : tableList) {
            if ((SdmSpInputOutputType.WENBENWENJIAN == SdmSpInputOutputType.ofEnumByCode(tableInfo.get("input_type").toString()))) {
                Map<String, Object> map = new HashMap<>();
                map.put("id", tableInfo.get("sdm_info_id"));
                map.put("label", tableInfo.get("input_table_name"));
                map.put("parent_id", KafkaConstant.WENBEN_FILE);
                tableMapNodes.add(map);
            } else if ((SdmSpInputOutputType.SHUJUKUBIAO == SdmSpInputOutputType.ofEnumByCode(tableInfo.get("input_type").toString()))) {
                Map<String, Object> map = new HashMap<>();
                map.put("id", tableInfo.get("sdm_info_id"));
                map.put("label", tableInfo.get("input_table_name"));
                map.put("parent_id", KafkaConstant.DATA_BASE_TABLE);
                tableMapNodes.add(map);
            } else if ((SdmSpInputOutputType.XIAOFEIZHUTI == SdmSpInputOutputType.ofEnumByCode(tableInfo.get("input_type").toString()))) {
                Map<String, Object> map = new HashMap<>();
                map.put("id", tableInfo.get("sdm_info_id"));
                map.put("label", tableInfo.get("input_table_name"));
                map.put("parent_id", KafkaConstant.CONSUMER_TOPIC);
                tableMapNodes.add(map);
            }
        }
        return tableMapNodes;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "analyseTableList", desc = "", range = "")
    public static List<Map<String, Object>> conversionAnalyseTableInfos(List<Map<String, Object>> analyseTableList) {
        List<Map<String, Object>> anaTableMapNodes = new ArrayList<>();
        for (Map<String, Object> anaTableInfo : analyseTableList) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", anaTableInfo.get("ssa_info_id"));
            map.put("label", anaTableInfo.get("analysis_table_name"));
            map.put("parent_id", KafkaConstant.ANALYSE_RESULT);
            anaTableMapNodes.add(map);
        }
        return anaTableMapNodes;
    }
}
