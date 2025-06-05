package hyren.serv6.s.datastore;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DatabaseInfo;
import hyren.serv6.commons.utils.database.bean.DBConnectionProp;
import hyren.serv6.s.bean.StoreConnectionBean;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataStoreLayer")
@Api(tags = "")
@Validated
public class DataStoreController {

    @Autowired
    DataStoreService dataStoreService;

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dsl_name", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "store_type", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "is_hadoopclient", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_remark", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_source", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_goal", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = long.class, example = ""), @ApiImplicitParam(name = "dataStoreLayerAttr", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsla_storelayer", value = "", dataTypeClass = String[].class, example = ""), @ApiImplicitParam(name = "files", value = "", dataTypeClass = MultipartFile[].class, example = "") })
    @RequestMapping("/addDataStore")
    public void addDataStore(String dsl_name, String store_type, String is_hadoopclient, @Nullable String dsl_remark, String dsl_source, String dsl_goal, String dataStoreLayerAttr, @Nullable String[] dsla_storelayer, @Nullable MultipartFile[] files) {
        dataStoreService.addDataStore(dsl_name, store_type, is_hadoopclient, dsl_remark, dsl_source, dsl_goal, dataStoreLayerAttr, dsla_storelayer, files);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dsl_name", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "store_type", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "is_hadoopclient", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_remark", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_source", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_goal", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = long.class, example = ""), @ApiImplicitParam(name = "dataStoreLayerAttr", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsla_storelayer", value = "", dataTypeClass = String[].class, example = ""), @ApiImplicitParam(name = "files", value = "", dataTypeClass = MultipartFile[].class, example = "") })
    @RequestMapping("/updateDataStore")
    public void updateDataStore(String dsl_name, String store_type, String is_hadoopclient, @Nullable String dsl_remark, String dsl_source, String dsl_goal, long dsl_id, String dataStoreLayerAttr, @Nullable String[] dsla_storelayer, @Nullable MultipartFile[] files) {
        dataStoreService.updateDataStore(dsl_name, store_type, is_hadoopclient, dsl_remark, dsl_source, dsl_goal, dsl_id, dataStoreLayerAttr, dsla_storelayer, files);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = long.class, example = "")
    @RequestMapping("/deleteDataStore")
    public void deleteDataStore(long dsl_id) {
        dataStoreService.deleteDataStore(dsl_id);
    }

    @ApiOperation(value = "")
    @RequestMapping("/searchDataStore")
    public Result searchDataStore() {
        return dataStoreService.searchDataStore("select * from " + DataStoreLayer.TableName);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = long.class, example = "")
    @RequestMapping("/searchDataStoreById")
    public String searchDataStoreById(long dsl_id) {
        return dataStoreService.searchDataStoreById(dsl_id);
    }

    @ApiOperation(value = "")
    @RequestMapping("/searchDBName")
    public Result searchDBName() {
        return dataStoreService.searchDBName("select database_name from " + DatabaseInfo.TableName);
    }

    @ApiOperation(value = "")
    @RequestMapping("/searchContrastTypeInfo")
    public Result searchContrastTypeInfo() {
        return dataStoreService.searchContrastTypeInfo();
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "db_name1", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "db_name2", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/getDataTypeMsg")
    public List<Map<String, Object>> getDataTypeMsg(String db_name1, String db_name2) {
        return dataStoreService.getDataTypeMsg(db_name1, db_name2);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "file", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("/uploadExcelFile")
    public void uploadExcelFile(String file) {
        dataStoreService.uploadExcelFile(file);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("/generateExcel")
    public void generateExcel(String fileName) {
        dataStoreService.generateExcel(fileName);
    }

    @ApiOperation(value = "")
    @RequestMapping("/downloadFile")
    public void downloadFile(String fileName) {
        dataStoreService.downloadFile(fileName);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dsl_id", value = "", dataTypeClass = long.class, example = ""), @ApiImplicitParam(name = "store_type", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "is_hadoopclient", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/getLayerAttrByIdAndType")
    public Result getLayerAttrByIdAndType(long dsl_id, String store_type, String is_hadoopclient) {
        return dataStoreService.getLayerAttrByIdAndType(dsl_id, store_type, is_hadoopclient);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "fileName", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "filePath", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/downloadConfFile")
    public void downloadConfFile(String fileName, String filePath) {
        dataStoreService.downloadConfFile(fileName, filePath);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "store_type", value = "", dataTypeClass = String.class, example = "")
    @RequestMapping("/getDataLayerAttrKey")
    public Map<String, Object> getDataLayerAttrKey(String store_type) {
        return dataStoreService.getDataLayerAttrKey(store_type);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "store_type", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "is_hadoopclient", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("getExternalTableAttrKey")
    public Map<String, Object> getExternalTableAttrKey(String store_type, String is_hadoopclient) {
        return dataStoreService.getExternalTableAttrKey(store_type, is_hadoopclient);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "store_type", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "is_hadoopclient", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "db_name", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/getAttrKeyByDatabaseType")
    public Map<String, Object> getAttrKeyByDatabaseType(String store_type, String is_hadoopclient, String db_name) {
        return dataStoreService.getAttrKeyByDatabaseType(store_type, is_hadoopclient, db_name);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParam(name = "storeConnectionBean", value = "", dataTypeClass = StoreConnectionBean.class)
    @RequestMapping("/testConnection")
    public void testConnection(StoreConnectionBean storeConnectionBean) {
        dataStoreService.testConnection(storeConnectionBean);
    }

    @ApiOperation(value = "")
    @RequestMapping("/getDBConnectionMsg")
    public DBConnectionProp getDBConnectionMsg(String db_name, String port) {
        return dataStoreService.getDBConnectionMsg(db_name, port);
    }

    @ApiOperation(value = "", tags = "")
    @RequestMapping("/testConnectionByDriver")
    @ApiImplicitParams({ @ApiImplicitParam(name = "file", value = "", dataTypeClass = MultipartFile.class), @ApiImplicitParam(name = "storeConnectionBean", value = "", dataTypeClass = StoreConnectionBean.class) })
    public void testConnectionByDriver(MultipartFile file, StoreConnectionBean storeConnectionBean) {
        dataStoreService.testConnectionByDriver(file, storeConnectionBean);
    }

    @ApiOperation(value = "", notes = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "dsl_source", value = "", dataTypeClass = String.class, example = ""), @ApiImplicitParam(name = "dsl_goal", value = "", dataTypeClass = String.class, example = "") })
    @RequestMapping("/searchDataStoreByDslType")
    public Result searchDataStoreByDslType(@Nullable String dsl_source, @Nullable String dsl_goal) {
        return dataStoreService.searchDataStoreByDslType(dsl_source, dsl_goal);
    }
}
