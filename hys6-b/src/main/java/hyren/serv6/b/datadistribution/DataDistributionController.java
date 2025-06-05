package hyren.serv6.b.datadistribution;

import hyren.serv6.b.datadistribution.bean.DistributeJobBean;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.entity.DataDistribute;
import hyren.serv6.base.entity.EtlJobDef;
import hyren.serv6.base.user.UserUtil;
import io.swagger.annotations.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@Api("数据分发操作类")
@Validated
@RequestMapping("/dataDistribution")
public class DataDistributionController {

    @Autowired
    DataDistributionService dataDistributionService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveDistributeData")
    @ApiImplicitParam(name = "data_distribute", value = "", dataTypeClass = DataDistribute.class)
    public void saveDistributeData(DataDistribute data_distribute) {
        dataDistributionService.saveDistributeData(data_distribute);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDistributeData")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", example = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", dataTypeClass = Integer.class) })
    public Map<String, Object> getDistributeData(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return dataDistributionService.getDistributeData(currPage, pageSize);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getIsReleaseData")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dd_ids", value = "", dataTypeClass = Long[].class), @ApiImplicitParam(name = "currPage", value = "", example = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", example = "", dataTypeClass = Integer.class) })
    @SuppressWarnings("unchecked")
    public Map<String, Object> getIsReleaseData(@RequestBody Map<String, Object> params) {
        List<String> dd_idsL = new ArrayList<>();
        Object obj = params.get("dd_ids");
        if (obj instanceof List) {
            dd_idsL = (List<String>) obj;
        } else if (obj instanceof String) {
            String dd_idsStr = (String) obj;
            dd_idsL.add(dd_idsStr);
        }
        Long[] dd_ids = new Long[dd_idsL.size()];
        for (int i = 0; i < dd_ids.length; i++) {
            dd_ids[i] = Long.parseLong(dd_idsL.get(i));
        }
        int currPage = 1;
        if (params.get("currPage") != null) {
            currPage = Integer.parseInt(params.get("currPage").toString());
        }
        int pageSize = 10;
        if (params.get("pageSize") != null) {
            pageSize = Integer.parseInt(params.get("pageSize").toString());
        }
        return dataDistributionService.getIsReleaseData(dd_ids, currPage, pageSize);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/queryAllColumnOnTableName")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "id", value = "", dataTypeClass = String.class) })
    public Map<String, Object> queryAllColumnOnTableName(@NotNull String source, @NotNull String id) {
        return dataDistributionService.queryAllColumnOnTableName(source, id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/uploadExcelFile")
    @ApiImplicitParam(name = "file", value = "", dataTypeClass = String.class)
    public void uploadExcelFile(@RequestBody MultipartFile file) {
        dataDistributionService.uploadExcelFile(file);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveDistributeDataJobRelation")
    @ApiImplicitParams({ @ApiImplicitParam(name = "job_defs", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "dd_id_list", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etlPre_job", value = "", dataTypeClass = String.class) })
    public void saveDistributeDataJobRelation(@RequestBody DistributeJobBean distributeJobBean) {
        List<List<String>> pre_etl_job_ids = distributeJobBean.getPreEtlJobIdList();
        List<Map<String, String>> dd_ids = distributeJobBean.getDdIds();
        List<EtlJobDef> relation = distributeJobBean.getEtlJobDefList();
        dataDistributionService.saveDistributeDataJobRelation(pre_etl_job_ids, dd_ids, relation);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteDistributeData")
    @ApiImplicitParam(name = "dd_id", value = "", dataTypeClass = Long.class)
    public void deleteDistributeData(@NotNull Long dd_id) {
        dataDistributionService.deleteDistributeData(dd_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDataInfoMsg")
    @ApiImplicitParam(name = "dd_id", value = "", dataTypeClass = Long.class)
    public DataDistribute getDataInfoMsg(@NotNull Long dd_id) {
        return dataDistributionService.getDataInfoMsg(dd_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateDistributeData")
    @ApiImplicitParam(name = "data_distribute", value = "", dataTypeClass = DataDistribute.class)
    public void updateDistributeData(@NotNull DataDistribute data_distribute) {
        dataDistributionService.updateDistributeData(data_distribute);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getJobMsg")
    @ApiImplicitParam(name = "dd_ids", value = "", dataTypeClass = Long[].class)
    @SuppressWarnings("unchecked")
    public List<List<Map<String, Object>>> getJobMsg(@RequestBody Map<String, Object> params) {
        List<String> dd_idsL = new ArrayList<>();
        Object obj = params.get("dd_ids");
        if (obj instanceof List) {
            dd_idsL = (List<String>) obj;
        } else if (obj instanceof String) {
            String dd_idsStr = (String) obj;
            dd_idsL.add(dd_idsStr);
        }
        Long[] dd_ids = new Long[dd_idsL.size()];
        for (int i = 0; i < dd_ids.length; i++) {
            dd_ids[i] = Long.parseLong(dd_idsL.get(i));
        }
        return dataDistributionService.getJobMsg(dd_ids);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/packageJars")
    @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class)
    public void packageJars(@NotNull String fileName) {
        dataDistributionService.packageJars(fileName);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/downloadDistributeFile")
    @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class)
    public void downloadDistributeFile(@NotNull String fileName) {
        dataDistributionService.downloadDistributeFile(fileName);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlJob")
    public List<String> searchEtlJob(long etl_sys_id) {
        return dataDistributionService.searchEtlJob(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getWebSQLTreeData")
    public List<Node> getWebSQLTreeData() {
        return dataDistributionService.getWebSQLTreeData();
    }
}
