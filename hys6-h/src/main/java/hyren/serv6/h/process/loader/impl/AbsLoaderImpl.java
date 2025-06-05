package hyren.serv6.h.process.loader.impl;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.entity.DmJobTableFieldInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.loader.ILoader;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;
import hyren.serv6.h.process.version.VersionManager;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static hyren.serv6.commons.utils.constant.Constant.HYRENFIELD;

@Slf4j
public abstract class AbsLoaderImpl implements ILoader {

    protected final Map<String, String> tableLayerAttrs = new HashMap<>();

    protected final ProcessJobTableConfBean processJobTableConfBean;

    protected final String tableName;

    protected final String etlDateWith8;

    protected final String moduleTableId;

    protected String createTableColumnTypes;

    protected IsFlag isZipperFlag = IsFlag.Fou;

    protected String currentTempTable;

    protected String zipperTempTable;

    protected String restoreTempTable;

    protected String moduleTableId_JobTableId;

    protected String jobTableId;

    protected static ProcessJobRunStatusEnum jobRunStatus = ProcessJobRunStatusEnum.UNKNOWN;

    protected List<String> partitionFields = new ArrayList<>();

    protected List<String> primaryKeyInfos = new ArrayList<>();

    protected VersionManager versionManager;

    protected String jobNameParam;

    protected StorageType storageType;

    protected List<DmJobTableFieldInfo> srcFieldInfos;

    protected AbsLoaderImpl(ProcessJobTableConfBean processJobTableConfBean) {
        this.processJobTableConfBean = processJobTableConfBean;
        this.tableName = processJobTableConfBean.getTarTableName();
        this.etlDateWith8 = processJobTableConfBean.getEtlDate();
        this.moduleTableId = processJobTableConfBean.getModuleTableId();
        this.moduleTableId_JobTableId = processJobTableConfBean.getModuleTableId() + "_" + processJobTableConfBean.getJobTableId();
        this.jobTableId = processJobTableConfBean.getJobTableId();
        processJobTableConfBean.getFieldAdditionalInfoMap().forEach((k, v) -> {
            StoreLayerAdded sla = StoreLayerAdded.ofEnumByCode(k);
            if (sla == StoreLayerAdded.ZhuJian) {
                this.primaryKeyInfos.addAll(v);
            }
            if (sla == StoreLayerAdded.FenQuLie) {
                this.partitionFields.addAll(v);
            }
        });
        this.srcFieldInfos = processJobTableConfBean.getDmJobTableFieldInfos().stream().filter(field -> !HYRENFIELD.contains(field.getJobtab_field_en_name().toUpperCase())).collect(Collectors.toList());
        this.jobNameParam = processJobTableConfBean.getJobNameParam();
        this.storageType = StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type());
        initTableLayerProperties();
        this.currentTempTable = tableName + "_current";
        this.zipperTempTable = tableName + "_zipper";
        this.restoreTempTable = tableName + "_restore";
        this.versionManager = new VersionManager(processJobTableConfBean);
    }

    @Override
    public ProcessJobTableConfBean getProcessJobTableConfBean() {
        return processJobTableConfBean;
    }

    @Override
    public void incrementalDataZipper() {
        throw new BusinessException("不支持进数方式! 需要具体实现 ILoader 接口的 incrementalDataZipper() " + StorageType.ZengLiang.getValue());
    }

    @Override
    public void handleException() {
        throw new BusinessException("Loader发生异常处理, 需要具体实现 ILoader 接口的 handleException() ");
    }

    protected DatabaseWrapper getDB() {
        return ConnectionTool.getDBWrapper(tableLayerAttrs);
    }

    private void initTableLayerProperties() {
        processJobTableConfBean.getDataStoreLayerAttrs().forEach(dsla -> tableLayerAttrs.put(dsla.getStorage_property_key(), dsla.getStorage_property_val()));
    }

    @Override
    public void close() {
        versionManager.commit();
        versionManager.close();
        getDB().close();
    }
}
