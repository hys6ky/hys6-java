package hyren.serv6.b.batchcollection.semiStructuredAgent.collectstoragelayerconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.resultset.Result;
import hyren.serv6.b.agent.bean.ColStoParam;
import hyren.serv6.b.agent.bean.DataStoRelaParam;
import hyren.serv6.base.entity.ObjectCollectStruct;
import hyren.serv6.base.entity.ObjectCollectTask;
import hyren.serv6.base.exception.BusinessException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Objects;

@RestController
@Slf4j
@RequestMapping("/dataCollectionO/semiStructuredAgent/collectstoragelayerconf")
@Api(value = "", tags = "")
public class CollectStorageLayerConfController {

    @Autowired
    public CollectStorageLayerConfService layerConfService;

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getCollectStorageLayerInfo")
    @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class)
    public Result getCollectStorageLayerInfo(@NotNull Long odc_id) {
        return layerConfService.getCollectStorageLayerInfo(odc_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getStorageLayerDestById")
    @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class)
    public Result getStorageLayerDestById(@NotNull Long ocs_id) {
        return layerConfService.getStorageLayerDestById(ocs_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getStorageLayerAttrById")
    @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class)
    public Result getStorageLayerAttrById(@NotNull Long dsl_id) {
        return layerConfService.getStorageLayerAttrById(dsl_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/getColumnStorageLayerInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class) })
    public Result getColumnStorageLayerInfo(@NotNull Long dsl_id, @NotNull Long ocs_id) {
        return layerConfService.getColumnStorageLayerInfo(dsl_id, ocs_id);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/saveColRelationStoreInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "ocs_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "colStoParams", value = "", dataTypeClass = ColStoParam[].class) })
    public void saveColRelationStoreInfo(@NotNull Long ocs_id, String colStoParams) {
        ColStoParam[] colStoParamsResult = JsonUtil.toObject(colStoParams, new TypeReference<ColStoParam[]>() {
        });
        layerConfService.saveColRelationStoreInfo(ocs_id, colStoParamsResult);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateColumnZhName")
    @ApiImplicitParam(name = "objectCollectStructs", value = "", dataTypeClass = ObjectCollectStruct[].class)
    public void updateColumnZhName(String objectCollectStructs) {
        ObjectCollectStruct[] objectCollectStructsResult = JsonUtil.toObject(objectCollectStructs, new TypeReference<ObjectCollectStruct[]>() {
        });
        layerConfService.updateColumnZhName(objectCollectStructsResult);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/batchSaveDtabRelationStoreInfo")
    @ApiImplicitParams({ @ApiImplicitParam(name = "odc_id", value = "", dataTypeClass = Long.class), @ApiImplicitParam(name = "dataStoRelaParams", value = "", dataTypeClass = DataStoRelaParam[].class) })
    public void batchSaveDtabRelationStoreInfo(@RequestBody Map<String, Object> req) {
        long odc_id = 0;
        String dataStoRelaParams = null;
        try {
            odc_id = Long.parseLong(req.get("odc_id").toString());
            dataStoRelaParams = Objects.isNull(req.get("dataStoRelaParams")) ? StringUtils.EMPTY : req.get("dataStoRelaParams").toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("req format failed.");
        }
        DataStoRelaParam[] dataStoRelaParamsResult = JsonUtil.toObject(dataStoRelaParams, new TypeReference<DataStoRelaParam[]>() {
        });
        layerConfService.batchSaveDtabRelationStoreInfo(odc_id, dataStoRelaParamsResult);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/updateTableZhName")
    @ApiImplicitParam(name = "objectCollectTasks", value = "", dataTypeClass = ObjectCollectTask[].class)
    public void updateTableZhName(@RequestBody Map<String, Object> req) {
        String objectCollectTasks = Objects.isNull(req.get("objectCollectTasks")) ? StringUtils.EMPTY : req.get("objectCollectTasks").toString();
        ObjectCollectTask[] objectCollectTasksResult = JsonUtil.toObject(objectCollectTasks, new TypeReference<ObjectCollectTask[]>() {
        });
        layerConfService.updateTableZhName(objectCollectTasksResult);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/searchDataStore")
    public Result searchDataStore() {
        return layerConfService.searchDataStore();
    }
}
