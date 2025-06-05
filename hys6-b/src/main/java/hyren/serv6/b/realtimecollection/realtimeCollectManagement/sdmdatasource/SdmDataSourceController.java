package hyren.serv6.b.realtimecollection.realtimeCollectManagement.sdmdatasource;

import fd.ng.core.annotation.Return;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.DataSource;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;

@RequestMapping("/dataCollectionM/sdmdatasource")
@RestController
@Validated
@Api("流数据管理源增删改查")
@Slf4j
public class SdmDataSourceController {

    @Autowired
    SdmDataSourceService sdmDataSourceService;

    @ApiOperation(tags = "", value = "")
    @Return(desc = "", range = "")
    @RequestMapping("/searchSdmDataSourceAndSdmAgentCount")
    public Result searchSdmDataSourceAndSdmAgentCount() {
        return sdmDataSourceService.searchSdmDataSourceAndSdmAgentCount();
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdmDataSource", value = "", dataTypeClass = DataSource.class)
    @RequestMapping("/saveSdmDataSource")
    public void saveSdmDataSource(DataSource sdmDataSource) {
        sdmDataSourceService.saveSdmDataSource(sdmDataSource);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParam(name = "sdm_source_id", value = "", dataTypeClass = Long.class)
    @RequestMapping("/deleteSdmDataSource")
    public void deleteSdmDataSource(@NotNull long sdm_source_id) {
        sdmDataSourceService.deleteSdmDataSource(sdm_source_id);
    }

    @ApiOperation(tags = "", value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "sdm_source_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "sdm_source_des", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_source_name", value = "", dataTypeClass = String.class), @ApiImplicitParam(name = "sdm_source_number", value = "", dataTypeClass = String.class) })
    @RequestMapping("/updateSdmDataSource")
    public void updateSdmDataSource(long sdm_source_id, String sdm_source_des, String sdm_source_name, String sdm_source_number) {
        sdmDataSourceService.updateSdmDataSource(sdm_source_id, sdm_source_des, sdm_source_name, sdm_source_number);
    }
}
