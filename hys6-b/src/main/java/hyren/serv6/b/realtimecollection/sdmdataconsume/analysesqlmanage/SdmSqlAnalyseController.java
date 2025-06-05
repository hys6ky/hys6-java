package hyren.serv6.b.realtimecollection.sdmdataconsume.analysesqlmanage;

import fd.ng.core.annotation.Return;
import hyren.serv6.b.agent.tools.ReqDataUtils;
import hyren.serv6.base.entity.SdmSpAnalysis;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

@RequestMapping("/dataCollectionO/sdmdataconsume/analysesqlmanage")
@RestController
@Slf4j
@Validated
@Api
public class SdmSqlAnalyseController {

    @Autowired
    SdmSqlAnalyseService sdmSqlAnalyseService;

    @ApiOperation(tags = "", value = "")
    @RequestMapping("/saveAnalyseSql")
    public void saveAnalyseSql(@RequestBody Map<String, Object> req) {
        List<String> analysis_sql = null;
        List<String> analysis_table_name = null;
        try {
            analysis_sql = (List<String>) req.get("analysis_sql");
            analysis_table_name = (List<String>) req.get("analysis_table_name");
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req data is illegal format ...");
        }
        long ssj_job_id = ReqDataUtils.getLongData(req, "ssj_job_id");
        sdmSqlAnalyseService.saveAnalyseSql(ssj_job_id, analysis_table_name, analysis_sql);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getAnalyseSql")
    public List<SdmSpAnalysis> getAnalyseSql(long ssj_job_id) {
        return sdmSqlAnalyseService.getAnalyseSql(ssj_job_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "ssa_info_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/deleteAnalyseSql")
    public void deleteAnalyseSql(long ssa_info_id) {
        sdmSqlAnalyseService.deleteAnalyseSql(ssa_info_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "pageStep", value = "", dataTypeClass = String.class)
    @Return(desc = "", range = "")
    @RequestMapping("/getSdmAnalyseDataInfos")
    public List<Map<String, Object>> getSdmAnalyseDataInfos(String pageStep) {
        return sdmSqlAnalyseService.getSdmAnalyseDataInfos(pageStep);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "ssj_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pageStep", value = "", dataTypeClass = String.class) })
    @Return(desc = "", range = "")
    @RequestMapping("/getTableDataList")
    public List<Map<String, Object>> getTableDataList(long ssj_job_id, String pageStep) {
        return sdmSqlAnalyseService.getTableDataList(ssj_job_id, pageStep);
    }
}
