package hyren.serv6.b.datasource;

import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.bean.SourceDepInfo;
import hyren.serv6.b.datasource.dto.UpdateSourceRelationDepDTO;
import hyren.serv6.base.entity.DataAuth;
import hyren.serv6.base.entity.DepartmentInfo;
import hyren.serv6.base.entity.SysUser;
import hyren.serv6.commons.utils.DboExecute;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/dataCollectionM")
@Validated
public class DataSourceController {

    @Autowired
    DataSourceServiceImpl dataSourceService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchDataSourceAndAgentCount")
    public Result searchDataSourceAndAgentCount() {
        return dataSourceService.searchDataSourceAndAgentCount();
    }

    @ApiOperation(value = "")
    @RequestMapping("/getTreeDataSourceAndAgentInfo")
    public List<Map<String, Object>> getTreeDataSourceAndAgentInfo() {
        return dataSourceService.getTreeDataSourceAndAgentInfo();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getDataAuditInfoForPage")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class) })
    public Result getDataAuditInfoForPage(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "5") Integer pageSize) {
        return dataSourceService.getDataAuditInfoForPage(currPage, pageSize);
    }

    @ApiOperation(value = "", tags = "")
    @PostMapping("/searchSourceRelationDepForPage")
    @ApiImplicitParams({ @ApiImplicitParam(name = "currPage", value = "", dataTypeClass = Integer.class), @ApiImplicitParam(name = "pageSize", value = "", dataTypeClass = Integer.class) })
    public Result searchSourceRelationDepForPage(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return dataSourceService.searchSourceRelationDepForPage(currPage, pageSize);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateAuditSourceRelationDep")
    @ApiImplicitParams({ @ApiImplicitParam(name = "source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "dep_id", value = "", dataTypeClass = Long[].class) })
    public void updateAuditSourceRelationDep(@RequestBody UpdateSourceRelationDepDTO dto) {
        dataSourceService.updateAuditSourceRelationDep(dto.getSource_id(), dto.getDep_id());
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/dataAudit")
    @ApiImplicitParams({ @ApiImplicitParam(name = "da_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "auth_type", value = "", dataTypeClass = String.class) })
    public void dataAudit(@NotNull Long da_id, String auth_type) {
        dataSourceService.dataAudit(da_id, auth_type);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteAudit")
    @ApiImplicitParam(name = "da_id", value = "", dataTypeClass = Long.class)
    public void deleteAudit(@NotNull Long da_id) {
        DboExecute.deletesOrThrow("权限回收失败!", "delete from " + DataAuth.TableName + " where da_id = ?", da_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveDataSource")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sourceDepInfo", value = "", dataTypeClass = SourceDepInfo.class) })
    public void saveDataSource(@RequestBody SourceDepInfo sourceDepInfo) {
        dataSourceService.saveDataSource(sourceDepInfo);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateDataSource")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sourceDepInfo", value = "", dataTypeClass = SourceDepInfo.class) })
    public void updateDataSource(@RequestBody SourceDepInfo sourceDepInfo) {
        dataSourceService.updateDataSource(sourceDepInfo);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchDataSourceById")
    @ApiImplicitParam(name = "source_id", value = "", dataTypeClass = Long.class)
    public Map<String, Object> searchDataSourceById(@NotNull Long source_id) {
        return dataSourceService.searchDataSourceById(source_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchDepartmentInfo")
    public List<DepartmentInfo> searchDepartmentInfo() {
        return Dbo.queryList(DepartmentInfo.class, "select * from " + DepartmentInfo.TableName);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/deleteDataSource")
    @ApiImplicitParam(name = "source_id", value = "", dataTypeClass = Long.class)
    public void deleteDataSource(@NotNull Long source_id) {
        dataSourceService.deleteDataSource(source_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchDataCollectUser")
    public List<SysUser> searchDataCollectUser() {
        return dataSourceService.searchDataCollectUser();
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/uploadFile")
    @ApiImplicitParams({ @ApiImplicitParam(name = "agent_ip", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "agent_port", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "user_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "file", value = "", dataTypeClass = String.class) })
    public void uploadFile(String agent_ip, String agent_port, Long user_id, MultipartFile file) {
        dataSourceService.uploadFile(agent_ip, agent_port, user_id, file);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/downloadFile")
    @ApiImplicitParam(name = "source_id", value = "", dataTypeClass = Long.class)
    public void downloadFile(@NotNull Long source_id) {
        dataSourceService.downloadFile(source_id);
    }
}
