package hyren.serv6.h.process.args;

import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.Store_type;
import lombok.Getter;
import java.io.Serializable;

@Getter
public class HandleArgs implements Serializable {

    private static final long serialVersionUID = 948741556567281755L;

    private Store_type storeType;

    private StorageType storageType;

    private IsFlag isTempFlag;

    private String tableName;

    private String datatableId;

    private String etlDateWith8;

    private String moduleTableId_JobTableId;

    private String doiId;

    private String jobNameParam;

    private String srcColumn;

    public static HandleArgs handleStringToHandleClass(String handleString, Class<? extends HandleArgs> aClass) throws Exception {
        return JsonUtil.toObjectSafety(handleString, aClass).orElseThrow(() -> new Exception(String.format("handle配置转handle对象失败: [ %s ] -> [ %s ].", handleString, aClass.getSimpleName())));
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }

    public void setStoreType(Store_type storeType) {
        this.storeType = storeType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public void setIsTempFlag(IsFlag isTempFlag) {
        this.isTempFlag = isTempFlag;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDatatableId(String datatableId) {
        this.datatableId = datatableId;
    }

    public void setEtlDateWith8(String etlDateWith8) {
        this.etlDateWith8 = etlDateWith8;
    }

    public void setModuleTableId_JobTableId(String moduleTableId_JobTableId) {
        this.moduleTableId_JobTableId = moduleTableId_JobTableId;
    }

    public void setDoiId(String doiId) {
        this.doiId = doiId;
    }

    public void setJobNameParam(String jobNameParam) {
        this.jobNameParam = jobNameParam;
    }

    public void setSrcColumn(String srcColumn) {
        this.srcColumn = srcColumn;
    }
}
