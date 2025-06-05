package hyren.serv6.commons.sqloperation;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.DbConfBean;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@Slf4j
@Api(tags = "")
@RestController()
@RequestMapping("/sqloperation")
@Configuration
public class SqlExecuteAction {

    @ApiImplicitParams({ @ApiImplicitParam(name = "sql", value = "", example = ""), @ApiImplicitParam(name = "storageType", value = "", example = "") })
    @ApiResponse(code = 200, message = "")
    @PostMapping("/sqlExecute")
    public void sqlExecute(String sql, String storageType) {
        Validator.notBlank(sql, "执行SQL不能为空");
        Validator.notBlank(storageType, "存储层名称不能为空");
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + DataStoreLayer.TableName + " Where lower(dsl_name) = lower(?) ", storageType).orElseThrow(() -> new BusinessException("SQL查询异常"));
        if (countNum == 0) {
            throw new BusinessException("储存层名称(" + storageType + "),不存在");
        }
        List<Map<String, Object>> databaseInfo = Dbo.queryList("SELECT t2.storage_property_key,t2.storage_property_val FROM  " + "" + DataStoreLayer.TableName + " t1 JOIN " + DataStoreLayerAttr.TableName + " t2 on t1.dsl_id = t2.dsl_id where lower(t1.dsl_name) = lower(?)", storageType);
        DbConfBean confBean = new DbConfBean();
        databaseInfo.forEach(item -> item.forEach((k, v) -> {
            if (v.equals(StorageTypeKey.database_driver)) {
                confBean.setDatabase_drive(item.get("storage_property_val").toString());
            }
            if (v.equals(StorageTypeKey.jdbc_url)) {
                confBean.setJdbc_url(item.get("storage_property_val").toString());
            }
            if (v.equals(StorageTypeKey.user_name)) {
                confBean.setUser_name(item.get("storage_property_val").toString());
            }
            if (v.equals(StorageTypeKey.database_pwd)) {
                confBean.setDatabase_pad(item.get("storage_property_val").toString());
            }
            if (v.equals(StorageTypeKey.database_pwd)) {
                confBean.setDatabase_pad(item.get("storage_property_val").toString());
            }
            if (v.equals(StorageTypeKey.database_type)) {
                confBean.setDatabase_type(item.get("storage_property_val").toString());
            }
            if (v.equals(StorageTypeKey.database_name)) {
                confBean.setDatabase_name(item.get("storage_property_val").toString());
            }
        }));
        try (Statement statement = ConnectionTool.getDBWrapper(confBean).getConnection().createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        }
    }
}
