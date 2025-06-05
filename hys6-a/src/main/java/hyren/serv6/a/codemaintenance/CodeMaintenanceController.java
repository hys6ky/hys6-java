package hyren.serv6.a.codemaintenance;

import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.a.entity.OrigCodeInfoDto;
import hyren.serv6.base.entity.HyrenCodeInfo;
import hyren.serv6.base.entity.OrigCodeInfo;
import hyren.serv6.base.entity.OrigSysoInfo;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/codeMaintenance")
@Api(tags = "")
@Validated
public class CodeMaintenanceController {

    @Autowired
    CodeMaintenanceService codeMaintenanceService;

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/getCodeInfo")
    public Result getCodeInfo() {
        return codeMaintenanceService.getCodeInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "hyren_code_infos", value = "", dataTypeClass = HyrenCodeInfo[].class, example = "")
    @RequestMapping("/saveCodeInfo")
    public void saveCodeInfo(@RequestBody HyrenCodeInfo[] hyren_code_infos) {
        codeMaintenanceService.saveCodeInfo(hyren_code_infos);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "hyren_code_infos", value = "", dataTypeClass = HyrenCodeInfo[].class, example = "")
    @RequestMapping("/updateCodeInfo")
    public void updateCodeInfo(@RequestBody HyrenCodeInfo[] hyren_code_infos) {
        if (hyren_code_infos.length != 0) {
            Validator.notBlank("更新时编码分类不能为空", hyren_code_infos[0].getCode_classify());
            Dbo.execute("delete from " + HyrenCodeInfo.TableName + " where code_classify = ?", hyren_code_infos[0].getCode_classify());
            Dbo.execute("delete from " + OrigCodeInfo.TableName + " where code_classify = ?", hyren_code_infos[0].getCode_classify());
            saveCodeInfo(hyren_code_infos);
        } else {
            throw new BusinessException("编码行信息不能为空");
        }
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_classify", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("/deleteCodeInfo")
    public void deleteCodeInfo(String code_classify) {
        codeMaintenanceService.deleteCodeInfo(code_classify);
    }

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/getOrigSysInfo")
    public Result getOrigSysInfo() {
        return codeMaintenanceService.getOrigSysInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "orig_syso_info", value = "", dataTypeClass = OrigSysoInfo.class, example = "")
    @RequestMapping("/addOrigSysInfo")
    public void addOrigSysInfo(@RequestBody OrigSysoInfo orig_syso_info) {
        codeMaintenanceService.addOrigSysInfo(orig_syso_info);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "orig_sys_code", value = "", example = "", dataTypeClass = String.class)
    @RequestMapping("/getOrigCodeInfo")
    public Result getOrigCodeInfo(String orig_sys_code) {
        codeMaintenanceService.isOrigSysoInfoExist(orig_sys_code);
        return codeMaintenanceService.getOrigCodeInfo(orig_sys_code);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "code_classify", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("getCodeInfoByCodeClassify")
    public List<Map<String, Object>> getCodeInfoByCodeClassify(String code_classify) {
        return codeMaintenanceService.getCodeInfoByCodeClassify(code_classify);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "orig_code_infos", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "orig_sys_code", value = "", dataTypeClass = String.class, example = "") })
    @PostMapping("/addOrigCodeInfo")
    public void addOrigCodeInfo(@RequestBody Map<String, Object> params) {
        String origCodeInfoStr = params.get("orig_code_infos").toString();
        String orig_sys_code = params.get("orig_sys_code").toString();
        List<OrigCodeInfoDto> list = codeMaintenanceService.stringToOrigCodeInfoDto(origCodeInfoStr);
        if (list.size() > 0) {
            codeMaintenanceService.addOrigCodeInfo(list, orig_sys_code);
        } else {
            throw new BusinessException("传入参数有误");
        }
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "orig_code_infos", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "orig_sys_code", value = "", dataTypeClass = String.class, example = "") })
    @PostMapping("/updateOrigCodeInfo")
    public void updateOrigCodeInfo(@RequestBody Map<String, Object> params) {
        String origCodeInfoStr = params.get("orig_code_infos").toString();
        String orig_sys_code = params.get("orig_sys_code").toString();
        List<OrigCodeInfoDto> list = codeMaintenanceService.stringToOrigCodeInfoDto(origCodeInfoStr);
        if (list.size() > 0) {
            codeMaintenanceService.updateOrigCodeInfo(list, orig_sys_code);
        } else {
            throw new BusinessException("传入参数有误");
        }
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "orig_sys_code", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "code_classify", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/deleteOrigCodeInfo")
    public void deleteOrigCodeInfo(String orig_sys_code, String code_classify) {
        codeMaintenanceService.isOrigSysoInfoExist(orig_sys_code);
        codeMaintenanceService.isHyrenCodeInfoExist(code_classify);
        codeMaintenanceService.deleteOrigCodeInfo(orig_sys_code, code_classify);
    }

    @ApiOperation(value = "", notes = "")
    @RequestMapping("/getAllCodeClassify")
    public List<String> getAllCodeClassify() {
        return codeMaintenanceService.getAllCodeClassify();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "orig_sys_code", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "code_classify", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/getOrigCodeInfoByCode")
    public Result getOrigCodeInfoByCode(String orig_sys_code, String code_classify) {
        return codeMaintenanceService.getOrigCodeInfoByCode(orig_sys_code, code_classify);
    }
}
