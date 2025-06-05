package hyren.serv6.c.etlsys;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.c.etlsys.dto.EtlSysAddDTO;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.base.entity.EtlSys;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import io.swagger.annotations.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.io.File;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

@Api(tags = "")
@RestController
@RequestMapping("etlMage")
@Slf4j
public class EtlSysController {

    @Autowired
    private EtlSysService service;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_cd", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_sys_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "comments", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "etl_sys_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "comments", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "pre_etl_sys_cds", value = "", required = true, dataTypeClass = Array.class), @ApiImplicitParam(name = "status", value = "", required = true, dataTypeClass = String.class) })
    @PostMapping("addEtlSys")
    public void addEtlSys(@RequestBody EtlSysAddDTO etlSysAddDTO) {
        EtlSys etl_sys = new EtlSys();
        etl_sys.setEtl_sys_cd(etlSysAddDTO.getEtl_sys_cd());
        etl_sys.setEtl_sys_name(etlSysAddDTO.getEtl_sys_name());
        etl_sys.setComments(etlSysAddDTO.getComments());
        String status = etlSysAddDTO.getStatus();
        Long[] pre_etl_sys_ids = etlSysAddDTO.getPre_etl_sys_ids();
        service.addEtlSys(etl_sys, status, pre_etl_sys_ids, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @PostMapping("deleteEtlProject")
    public void deleteEtlProject(Long etl_sys_id) {
        service.deleteEtlProject(etl_sys_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_sys_cd", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_serv_ip", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "serv_file_path", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "user_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "user_pwd", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "isCustomize", value = "", dataTypeClass = String.class) })
    @PostMapping("deployEtlJobScheduleProject")
    public void deployEtlJobScheduleProject(@RequestParam(name = "etl_sys_id", required = true) Long etl_sys_id, @RequestParam(name = "etl_sys_cd", required = true) String etl_sys_cd, @RequestParam(name = "etl_serv_ip", required = true) String etl_serv_ip, @RequestParam(name = "serv_file_path", required = false) String serv_file_path, @RequestParam(name = "user_name", required = false) String user_name, @RequestParam(name = "user_pwd", required = false) String user_pwd, @RequestParam(name = "isCustomize", required = true) String isCustomize) {
        service.deployEtlJobScheduleProject(etl_sys_id, etl_sys_cd, etl_serv_ip, serv_file_path, user_name, user_pwd, isCustomize, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_cd", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "isControl", value = "", dataTypeClass = String.class) })
    @PostMapping("downloadControlOrTriggerLog")
    public String downloadControlOrTriggerLog(@RequestParam(name = "etl_sys_id", required = true) Long etl_sys_id, @RequestParam(name = "etl_sys_cd", required = true) String etl_sys_cd, @RequestParam(name = "curr_bath_date", required = true) String curr_bath_date, @RequestParam(name = "isControl", required = true) String isControl) {
        return service.downloadControlOrTriggerLog(etl_sys_id, etl_sys_cd, curr_bath_date, isControl, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("getEtlSysDepById")
    public List<Map<String, Object>> getEtlSysDepById(@RequestParam(name = "etl_sys_id", required = true) Long etl_sys_id) {
        return service.getEtlSysDepById(etl_sys_id);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_cd", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "readNum", value = "", defaultValue = "100", dataTypeClass = Integer.class), @ApiImplicitParam(name = "isControl", value = "", dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("readControlOrTriggerLog")
    public String readControlOrTriggerLog(@RequestParam(name = "etl_sys_id") Long etl_sys_id, @RequestParam(name = "etl_sys_cd") String etl_sys_cd, @RequestParam(name = "readNum", defaultValue = "100") Integer readNum, @RequestParam(name = "isControl", required = true) String isControl) {
        return service.readControlOrTriggerLog(etl_sys_id, etl_sys_cd, readNum, isControl, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlSys")
    public Result searchEtlSys() {
        return service.searchEtlSys(UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchEtlSysById")
    public Map<String, Object> searchEtlSysById(Long etl_sys_id) {
        return service.searchEtlSysById(etl_sys_id, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "tableName", value = "", dataTypeClass = String.class) })
    @ApiResponses({ @ApiResponse(code = 200, message = "") })
    @PostMapping("searchTable")
    public Result searchTable(@RequestParam(name = "tableName", required = true) String tableName) {
        return service.searchTable(tableName);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "isResumeRun", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "isAutoShift", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "curr_bath_date", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sys_end_date", value = "", dataTypeClass = String.class) })
    @PostMapping("startControl")
    public void startControl(@RequestParam(name = "etl_sys_id", required = true) Long etl_sys_id, @RequestParam(name = "etl_sys_cd", required = true) String etl_sys_cd, @RequestParam(name = "isResumeRun") String isResumeRun, @RequestParam(name = "isAutoShift") String isAutoShift, @RequestParam(name = "curr_bath_date") String curr_bath_date, @RequestParam(name = "sys_end_date") String sys_end_date) {
        service.startControl(etl_sys_id, etl_sys_cd, isResumeRun, isAutoShift, curr_bath_date, sys_end_date, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_sys_cd", value = "", dataTypeClass = String.class) })
    @PostMapping("startTrigger")
    public void startTrigger(@RequestParam(name = "etl_sys_id", required = true) Long etl_sys_id, @RequestParam(name = "etl_sys_cd", required = true) String etl_sys_cd) {
        service.startTrigger(etl_sys_id, etl_sys_cd, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "etl_sys_cd", value = "", dataTypeClass = String.class) })
    @PostMapping("stopEtlProject")
    public void stopEtlProject(@RequestParam(name = "etl_sys_id", required = true) Long etl_sys_id, @RequestParam(name = "etl_sys_cd", required = true) String etl_sys_cd) {
        service.stopEtlProject(etl_sys_id, etl_sys_cd);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "etl_sys_cd", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "etl_sys_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "comments", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "status", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "pre_etl_sys_cds", value = "", required = true, dataTypeClass = Array.class), @ApiImplicitParam(name = "etl_sys_id", value = "", required = true, dataTypeClass = Long.class) })
    @PostMapping("updateEtlSys")
    public void updateEtlSys(@RequestBody EtlSysAddDTO etlSysAddDTO) {
        Long etl_sys_id = etlSysAddDTO.getEtl_sys_id();
        String etl_sys_cd = etlSysAddDTO.getEtl_sys_cd();
        String etl_sys_name = etlSysAddDTO.getEtl_sys_name();
        String comments = etlSysAddDTO.getComments();
        String status = etlSysAddDTO.getStatus();
        Long[] pre_etl_sys_ids = etlSysAddDTO.getPre_etl_sys_ids();
        service.updateEtlSys(etl_sys_id, etl_sys_cd, etl_sys_name, comments, status, pre_etl_sys_ids, UserUtil.getUserId());
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class, example = "")
    @PostMapping("/downloadFile")
    public void downloadFile(String fileName) {
        String path = WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName;
        FileDownloadUtil.downloadFile(path);
    }
}
