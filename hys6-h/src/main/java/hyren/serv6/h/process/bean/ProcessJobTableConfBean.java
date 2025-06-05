package hyren.serv6.h.process.bean;

import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.*;
import hyren.serv6.commons.collection.bean.LayerBean;
import lombok.Getter;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
public class ProcessJobTableConfBean implements Serializable {

    private static final long serialVersionUID = -5764459740769055530L;

    public ProcessJobTableConfBean(String moduleTableId, String jobTableId, String etlDate, String jobNameParam, String sqlParams) {
        this.moduleTableId = moduleTableId;
        this.jobTableId = jobTableId;
        this.etlDate = etlDate;
        this.jobNameParam = jobNameParam;
        this.sqlParams = sqlParams;
    }

    private final String moduleTableId;

    private final String jobTableId;

    private final String etlDate;

    private final String jobNameParam;

    private final String sqlParams;

    private boolean reRun = Boolean.FALSE;

    private String tarTableName;

    private String completeSql;

    private String beforeReplaceSql;

    private boolean isGroup;

    private String preJobSql;

    private String postJobSql;

    private DmModuleTable dmModuleTable;

    private DmJobTableInfo dmJobTableInfo;

    private List<DmJobTableFieldInfo> dmJobTableFieldInfos;

    private DtabRelationStore dtabRelationStore;

    private DataStoreLayer dataStoreLayer;

    private List<DataStoreLayerAttr> dataStoreLayerAttrs;

    private Map<String, List<String>> fieldAdditionalInfoMap;

    private IsFlag isTempFlag;

    private IsFlag isZipperFlag;

    private Map<String, List<LayerBean>> layerBeansByTableMap;

    private List<String> partitionFields;

    private List<String> primaryKeyInfos;

    public void setReRun(boolean reRun) {
        this.reRun = reRun;
    }

    public void setTarTableName(String tarTableName) {
        this.tarTableName = tarTableName;
    }

    public void setCompleteSql(String completeSql) {
        this.completeSql = completeSql;
    }

    public void setBeforeReplaceSql(String beforeReplaceSql) {
        this.beforeReplaceSql = beforeReplaceSql;
    }

    public boolean isGroup() {
        return isGroup;
    }

    public void setGroup(boolean group) {
        isGroup = group;
    }

    public void setPreJobSql(String preJobSql) {
        this.preJobSql = preJobSql;
    }

    public void setPostJobSql(String postJobSql) {
        this.postJobSql = postJobSql;
    }

    public void setDmModuleTable(DmModuleTable dmModuleTable) {
        this.dmModuleTable = dmModuleTable;
    }

    public void setDmJobTableInfo(DmJobTableInfo dmJobTableInfo) {
        this.dmJobTableInfo = dmJobTableInfo;
    }

    public void setDmJobTableFieldInfos(List<DmJobTableFieldInfo> dmJobTableFieldInfos) {
        this.dmJobTableFieldInfos = dmJobTableFieldInfos;
    }

    public void setDtabRelationStore(DtabRelationStore dtabRelationStore) {
        this.dtabRelationStore = dtabRelationStore;
    }

    public void setDataStoreLayer(DataStoreLayer dataStoreLayer) {
        this.dataStoreLayer = dataStoreLayer;
    }

    public void setDataStoreLayerAttrs(List<DataStoreLayerAttr> dataStoreLayerAttrs) {
        this.dataStoreLayerAttrs = dataStoreLayerAttrs;
    }

    public void setFieldAdditionalInfoMap(Map<String, List<String>> fieldAdditionalInfoMap) {
        this.fieldAdditionalInfoMap = fieldAdditionalInfoMap;
    }

    public void setIsTempFlag(IsFlag isTempFlag) {
        this.isTempFlag = isTempFlag;
    }

    public void setIsZipperFlag(IsFlag isZipperFlag) {
        this.isZipperFlag = isZipperFlag;
    }

    public void setLayerBeansByTableMap(Map<String, List<LayerBean>> layerBeansByTableMap) {
        this.layerBeansByTableMap = layerBeansByTableMap;
    }

    public void setPartitionFields(List<String> partitionFields) {
        this.partitionFields = partitionFields;
    }

    public void setPrimaryKeyInfos(List<String> primaryKeyInfos) {
        this.primaryKeyInfos = primaryKeyInfos;
    }
}
