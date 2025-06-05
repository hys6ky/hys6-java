package hyren.serv6.c.jobconfig;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.c.jobconfig.dto.*;
import io.swagger.annotations.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("etlMage/jobconfig")
@Slf4j
public class JobConfigController {

    @Autowired
    private JobConfigService service;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etlDependencies", value = "", dataTypeClass = String.class) })
    @PostMapping("batchDeleteEtlDependency")
    public void batchDeleteEtlDependency(@RequestParam("etlDependencies") String etlDependencies) {
        service.batchDeleteEtlDependency(etlDependencies, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pre_etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "status", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sub_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pre_sub_sys_id", value = "", dataTypeClass = Long.class) })
    @PostMapping("batchSaveEtlDependency")
    public void batchSaveEtlDependency(Long etl_sys_id, Long pre_etl_sys_id, Long sub_sys_id, Long pre_sub_sys_id, String status) {
        service.batchSaveEtlDependency(etl_sys_id, pre_etl_sys_id, sub_sys_id, pre_sub_sys_id, status, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pre_etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pre_etl_job_id", value = "", dataTypeClass = Long.class) })
    @PostMapping("deleteEtlDependency")
    public void deleteEtlDependency(Long etl_sys_id, Long pre_etl_sys_id, Long etl_job_id, Long pre_etl_job_id) {
        service.deleteEtlDependency(etl_sys_id, pre_etl_sys_id, etl_job_id, pre_etl_job_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job", value = "", dataTypeClass = Long.class) })
    @PostMapping("deleteEtlJobDef")
    public void deleteEtlJobDef(Long etl_sys_id, Long etl_job_id) {
        service.deleteEtlJobDef(etl_sys_id, etl_job_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "etl_sys_cd", desc = "", range = "")
    @Param(name = "etl_job", desc = "", range = "")
    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = Long.class) })
    @PostMapping("/batchDeleteEtlJobDef")
    public void batchDeleteEtlJobDef(@RequestBody EtlDependencyDTO dto) {
        service.batchDeleteEtlJobDef(dto.getEtl_sys_id(), dto.getEtl_job_ids());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job", value = "", dataTypeClass = String.class) })
    @PostMapping("deleteEtlJobResourceRela")
    public void deleteEtlJobResourceRela(Long etl_sys_id, Long etl_job_id) {
        service.deleteEtlJobResourceRela(etl_sys_id, etl_job_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "para_cd", value = "", dataTypeClass = String.class) })
    @PostMapping("deleteEtlPara")
    public void deleteEtlPara(Long etl_sys_id, String para_cd) {
        service.deleteEtlPara(etl_sys_id, para_cd);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "resource_type", value = "", dataTypeClass = String.class) })
    @PostMapping("deleteEtlResource")
    public void deleteEtlResource(Long etl_sys_id, String resource_type) {
        service.deleteEtlResource(etl_sys_id, resource_type);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sub_sys_id", value = "", dataTypeClass = Long.class) })
    @PostMapping("deleteEtlSubSys")
    public void deleteEtlSubSys(Long etl_sys_id, Long sub_sys_id) {
        service.deleteEtlSubSys(etl_sys_id, sub_sys_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class) })
    @PostMapping("generateExcel")
    public String generateExcel(Long etl_sys_id, String tableName) {
        return service.generateExcel(etl_sys_id, tableName, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "fileName", value = "")
    @PostMapping("/downloadFile")
    public void downloadFile(String fileName) {
        service.downloadFile(fileName);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "table_name", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/uploadExcelFile")
    public void uploadExcelFile(MultipartFile file, String table_name) {
        service.uploadExcelFile(file, table_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class, example = ""), @ApiImplicitParam(name = "sub_sys_id", value = "", dataTypeClass = Long.class, example = "") })
    @PostMapping("/batchDeleteEtlSubSys")
    public void batchDeleteEtlSubSys(Long etl_sys_id, Long[] sub_sys_ids) {
        service.batchDeleteEtlSubSys(etl_sys_id, sub_sys_ids);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_dependency", value = "", dataTypeClass = EtlDependency.class) })
    @PostMapping("saveEtlDependency")
    public void saveEtlDependency(@RequestBody EtlDependency etl_dependency) {
        service.saveEtlDependency(etl_dependency, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_error_resource", value = "", dataTypeClass = EtlErrorResource.class) })
    @PostMapping("saveEtlErrorResource")
    public void saveEtlErrorResource(EtlErrorResource etl_error_resource) {
        service.saveEtlErrorResource(etl_error_resource);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dto", value = "", required = true, dataTypeClass = EtlJobDefSaveDTO.class) })
    @PostMapping("saveEtlJobDef")
    public void saveEtlJobDef(@RequestBody EtlJobDefSaveDTO dto) {
        dto.getEtl_job_def().setEtl_job_id(PrimayKeyGener.getNextId());
        dto.getEtl_dependency().setEtl_job_id(dto.getEtl_job_def().getEtl_job_id());
        service.saveEtlJobDef(dto.getEtl_job_def(), dto.getEtl_dependency(), dto.getPre_etl_job_ids());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "jobResourceRelation", value = "", dataTypeClass = EtlJobResourceRela.class) })
    @PostMapping("saveEtlJobResourceRela")
    public void saveEtlJobResourceRela(EtlJobResourceRela jobResourceRelation) {
        service.saveEtlJobResourceRela(jobResourceRelation);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sub_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "job_datasource", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_temp_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_temp_para", value = "", dataTypeClass = Array.class) })
    @PostMapping("saveEtlJobTemp")
    public void saveEtlJobTemp(Long etl_sys_id, Long sub_sys_id, String etl_job, String job_datasource, Long etl_temp_id, String[] etl_job_temp_para) {
        service.saveEtlJobTemp(etl_sys_id, sub_sys_id, etl_job, job_datasource, etl_temp_id, etl_job_temp_para);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_para", value = "", dataTypeClass = EtlPara.class) })
    @PostMapping("saveEtlPara")
    public void saveEtlPara(EtlPara etl_para) {
        service.saveEtlPara(etl_para, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_resource", value = "", dataTypeClass = EtlResource.class) })
    @PostMapping("saveEtlResource")
    public void saveEtlResource(EtlResource etl_resource) {
        service.saveEtlResource(etl_resource, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sub_sys_list", value = "", dataTypeClass = EtlSubSysList.class) })
    @PostMapping("saveEtlSubSys")
    public void saveEtlSubSys(EtlSubSysList etl_sub_sys_list) {
        service.saveEtlSubSys(etl_sub_sys_list, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", required = true, dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "pre_etl_job", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "pageType", value = "", defaultValue = "", dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlDependencyByPage")
    public Map<String, Object> searchEtlDependencyByPage(Long etl_sys_id, Long etl_job_id, String etl_job, String pre_etl_job, String pageType, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return service.searchEtlDependencyByPage(etl_sys_id, etl_job_id, etl_job, pre_etl_job, pageType, currPage, pageSize, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlErrorResource")
    public Result searchEtlErrorResource(Long etl_sys_id) {
        return service.searchEtlErrorResource(etl_sys_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlJobTemplate")
    public Result searchEtlJobTemplate() {
        return service.searchEtlJobTemplate();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlJobDefById")
    public Map<String, Object> searchEtlJobDefById(Long etl_sys_id, Long etl_job_id) {
        return service.searchEtlJobDefById(etl_sys_id, etl_job_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pro_type", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "etl_job", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "pro_name", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "sub_sys_cd", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlJobDefByPage")
    public Map<String, Object> searchEtlJobDefByPage(Long etl_sys_id, String pro_type, String etl_job, String pro_name, String sub_sys_cd, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return service.searchEtlJobDefByPage(etl_sys_id, pro_type, etl_job, pro_name, sub_sys_cd, currPage, pageSize, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlJob")
    public List<Map<String, Object>> searchEtlJob(Long etl_sys_id) {
        return service.searchEtlJob(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("findJobByEtlSysId")
    public List<EtlJobDef> findJobByEtlSysId(Long etl_sys_id) {
        return service.findJobByEtlSysId(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "pageType", value = "", defaultValue = "", dataTypeClass = String.class), @ApiImplicitParam(name = "resource_type", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlJobResourceRelaByPage")
    public Map<String, Object> searchEtlJobResourceRelaByPage(@RequestBody EtlJobResourceRelaDTO dto) {
        return service.searchEtlJobResourceRelaByPage(dto.getEtl_sys_id(), dto.getEtl_job(), dto.getPageType(), dto.getResource_type(), dto.getCurrPage(), dto.getPageSize(), UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_temp_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlJobTempAndParam")
    public List<Map<String, Object>> searchEtlJobTempAndParam(long etl_temp_id) {
        return service.searchEtlJobTempAndParam(etl_temp_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "para_cd", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlParaByPage")
    public Map<String, Object> searchEtlParaByPage(Long etl_sys_id, String para_cd, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return service.searchEtlParaByPage(etl_sys_id, para_cd, currPage, pageSize, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "resource_type", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlResourceByPage")
    public Map<String, Object> searchEtlResourceByPage(Long etl_sys_id, String resource_type, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return service.searchEtlResourceByPage(etl_sys_id, resource_type, currPage, pageSize, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlResourceType")
    public List<String> searchEtlResourceType(Long etl_sys_id) {
        return service.searchEtlResourceType(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sub_sys_cd", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "currPage", value = "", defaultValue = "1", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", defaultValue = "10", dataTypeClass = Integer.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlSubSysByPage")
    public Map<String, Object> searchEtlSubSysByPage(Long etl_sys_id, String sub_sys_cd, @RequestParam(name = "currPage", defaultValue = "1") Integer currPage, @RequestParam(name = "pageSize", defaultValue = "10") Integer pageSize) {
        return service.searchEtlSubSysByPage(etl_sys_id, sub_sys_cd, currPage, pageSize, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlSubSys")
    public List<EtlSubSysList> searchEtlSubSys(Long etl_sys_id) {
        return service.searchEtlSubSys(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_ids", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchJobDependency")
    public Map<String, Long> searchJobDependency(Long[] etl_job_ids, Long etl_sys_id) {
        return service.searchJobDependency(etl_sys_id, etl_job_ids);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_ids", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchJobDependency1")
    public Map<String, Long> searchJobDependency1(@RequestBody EtlDependencyDTO etlDependencyDTO) {
        return service.searchJobDependency(etlDependencyDTO.getEtl_sys_id(), etlDependencyDTO.getEtl_job_ids());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etlDependency", value = "", dataTypeClass = EtlDependency.class), @ApiImplicitParam(name = "oldEtlJobId", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "oldPreEtlJobId", value = "", dataTypeClass = Long.class) })
    @PostMapping("updateEtlDependency")
    public void updateEtlDependency(EtlDependency etlDependency, Long oldEtlJobId, Long oldPreEtlJobId) {
        service.updateEtlDependency(etlDependency, oldEtlJobId, oldPreEtlJobId, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dto", value = "", required = true, dataTypeClass = EtlJobDefUpdateDTO.class) })
    @PostMapping("updateEtlJobDef")
    public void updateEtlJobDef(@RequestBody EtlJobDefUpdateDTO dto) {
        service.updateEtlJobDef(dto.getEtl_job_def(), dto.getEtl_dependency(), dto.getOld_disp_freq(), dto.getOld_pre_etl_job_ids(), dto.getOld_dispatch_type(), dto.getPre_etl_job_ids());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "jobResourceRelation", value = "", dataTypeClass = String.class) })
    @PostMapping("updateEtlJobResourceRela")
    public void updateEtlJobResourceRela(EtlJobResourceRela jobResourceRelation) {
        service.updateEtlJobResourceRela(jobResourceRelation);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_para", value = "", dataTypeClass = EtlPara.class) })
    @PostMapping("updateEtlPara")
    public void updateEtlPara(EtlPara etl_para) {
        service.updateEtlPara(etl_para, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "resource_type", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "resource_max", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "resource_name", value = "", required = true, dataTypeClass = String.class) })
    @PostMapping("updateEtlResource")
    public void updateEtlResource(Long etl_sys_id, String resource_type, Long resource_max, String resource_name) {
        if (resource_max == null || resource_max <= 0) {
            throw new BusinessException("资源阀值必须是大于0的正整数");
        }
        service.updateEtlResource(etl_sys_id, resource_type, resource_max, resource_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sub_sys_list", value = "", dataTypeClass = EtlSubSysList.class) })
    @PostMapping("updateEtlSubSys")
    public void updateEtlSubSys(EtlSubSysList etl_sub_sys_list) {
        service.updateEtlSubSys(etl_sub_sys_list, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_job_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "pro_type", value = "", dataTypeClass = String.class) })
    @PostMapping("saveEtlJobResource")
    public void saveEtlJobResource(Long etl_sys_id, Long etl_job_id, String pro_type) {
        service.saveEtlJobResource(etl_sys_id, etl_job_id, pro_type);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", example = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "resource_type", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/batchDeleteEtlResource")
    public void batchDeleteEtlResource(Long etl_sys_id, String resource_type) {
        service.batchDeleteEtlResource(etl_sys_id, resource_type);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", example = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_job_id", value = "", example = "", dataTypeClass = String.class) })
    @PostMapping("/batchDeleteEtlJobResourceRela")
    public void batchDeleteEtlJobResourceRela(Long etl_sys_id, String etl_job_id) {
        service.batchDeleteEtlJobResourceRela(etl_sys_id, etl_job_id);
    }

    @ApiOperation(value = "", notes = "")
    @PostMapping("/batchDeleteEtlPara")
    public void batchDeleteEtlPara(@RequestBody BatchDeleteEtlParaDTO dto) {
        service.batchDeleteEtlPara(dto);
    }
}
