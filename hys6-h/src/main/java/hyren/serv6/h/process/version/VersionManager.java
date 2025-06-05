package hyren.serv6.h.process.version;

import fd.ng.core.utils.BeanUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class VersionManager implements Closeable {

    private final DatabaseWrapper db;

    private final List<DmJobTableFieldInfo> fields;

    private final String tarTableName;

    private final String etlDate;

    private final Long jobTableId;

    private final Long moduleTableId;

    public VersionManager(ProcessJobTableConfBean processJobTableConfBean) {
        this.fields = processJobTableConfBean.getDmJobTableFieldInfos();
        this.tarTableName = processJobTableConfBean.getTarTableName();
        this.etlDate = processJobTableConfBean.getEtlDate();
        this.jobTableId = Long.parseLong(processJobTableConfBean.getJobTableId());
        this.moduleTableId = Long.parseLong(processJobTableConfBean.getModuleTableId());
        db = new DatabaseWrapper();
        db.beginTrans();
    }

    public boolean isVersionExpire() {
        return false;
    }

    public String getRenameTableName() {
        return tarTableName + "_" + etlDate.substring(2);
    }

    public void updateJobVersion() {
        DmJobTableInfo runJob = SqlOperator.queryOneObject(db, DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where module_table_id = ? and jobtab_id = ?", moduleTableId, jobTableId).orElseThrow(() -> new BusinessException("find job failed."));
        DmJobTableVersionInfo versionJob = SqlOperator.queryOneObject(db, DmJobTableVersionInfo.class, "select * from " + DmJobTableVersionInfo.TableName + " where module_table_id = ? and jobtab_id = ? and version_date = ?", moduleTableId, jobTableId, etlDate).orElse(null);
        if (versionJob != null) {
            if (checkJobTabAndField(runJob, versionJob)) {
                log.info(" job table fields changes, update begin ... jobtabId: {}", jobTableId);
                db.execute("delete from " + DmJobTableFieldVersionInfo.TableName + " where jobtab_id = ? and version_date = ?", jobTableId, etlDate);
                db.execute("delete from " + DmJobTableVersionInfo.TableName + " where jobtab_id = ? and version_date = ?", jobTableId, etlDate);
            } else {
                return;
            }
        }
        DmJobTableVersionInfo versionInfo = new DmJobTableVersionInfo();
        BeanUtils.copyProperties(runJob, versionInfo);
        versionInfo.setVersion_date(etlDate);
        versionInfo.setJobtab_version_id(PrimayKeyGener.getNextId());
        versionInfo.add(db);
        List<DmJobTableFieldInfo> jobFields = SqlOperator.queryList(db, DmJobTableFieldInfo.class, "select * from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ?", jobTableId);
        for (DmJobTableFieldInfo jobField : jobFields) {
            DmJobTableFieldVersionInfo dmJobTableFieldVersionInfo = new DmJobTableFieldVersionInfo();
            BeanUtils.copyProperties(jobField, dmJobTableFieldVersionInfo);
            dmJobTableFieldVersionInfo.setJobtab_field_version_id(PrimayKeyGener.getNextId());
            dmJobTableFieldVersionInfo.setVersion_date(etlDate);
            dmJobTableFieldVersionInfo.add(db);
        }
    }

    private boolean checkJobTabAndField(DmJobTableInfo runJob, DmJobTableVersionInfo versionJob) {
        boolean viewSql = versionJob.getJobtab_view_sql().equals(runJob.getJobtab_view_sql());
        boolean execSql = versionJob.getJobtab_execute_sql().equals(runJob.getJobtab_execute_sql());
        if (viewSql && execSql) {
            List<DmJobTableFieldInfo> jobFields = SqlOperator.queryList(db, DmJobTableFieldInfo.class, "select * from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ?", jobTableId);
            List<DmJobTableFieldVersionInfo> versionFields = SqlOperator.queryList(db, DmJobTableFieldVersionInfo.class, "select * from " + DmJobTableFieldVersionInfo.TableName + " where jobtab_id = ? and version_date = ?", jobTableId, etlDate);
            if (jobFields.size() != versionFields.size()) {
                return true;
            }
            for (DmJobTableFieldInfo jobField : jobFields) {
                Long jobtabFieldId = jobField.getJobtab_field_id();
                List<DmJobTableFieldVersionInfo> nameEqualBeans = versionFields.stream().filter(dmJobTableFieldVersionInfo -> dmJobTableFieldVersionInfo.getJobtab_field_id().equals(jobtabFieldId)).collect(Collectors.toList());
                if (nameEqualBeans.size() != 1) {
                    return true;
                }
                DmJobTableFieldVersionInfo nameEqualBean = nameEqualBeans.get(0);
                boolean flag = true;
                if (jobField.getJobtab_field_en_name() != null) {
                    flag = jobField.getJobtab_field_en_name().equals(nameEqualBean.getJobtab_field_en_name());
                }
                if (jobField.getJobtab_field_cn_name() != null) {
                    flag = jobField.getJobtab_field_cn_name().equals(nameEqualBean.getJobtab_field_cn_name());
                }
                if (jobField.getJobtab_field_process() != null) {
                    flag = jobField.getJobtab_field_process().equals(nameEqualBean.getJobtab_field_process());
                }
                if (jobField.getJobtab_field_seq() != null) {
                    flag = jobField.getJobtab_field_seq().equals(nameEqualBean.getJobtab_field_seq());
                }
                if (jobField.getJobtab_field_type() != null) {
                    flag = jobField.getJobtab_field_type().equals(nameEqualBean.getJobtab_field_type());
                }
                if (jobField.getJobtab_field_length() != null) {
                    flag = jobField.getJobtab_field_length().equals(nameEqualBean.getJobtab_field_length());
                }
                if (jobField.getJobtab_process_mapping() != null) {
                    flag = jobField.getJobtab_process_mapping().equals(nameEqualBean.getJobtab_process_mapping());
                }
                if (jobField.getJobtab_group_mapping() != null) {
                    flag = jobField.getJobtab_group_mapping().equals(nameEqualBean.getJobtab_group_mapping());
                }
                return !flag;
            }
        }
        return !(viewSql && execSql);
    }

    public void updateSqlVersion() {
        Optional<DmJobTableInfo> dmti = SqlOperator.queryOneObject(db, DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where module_table_id = ? and version_date <> ?", jobTableId, Constant._MAX_DATE_8);
        if (dmti.isPresent()) {
            Optional<DmJobTableInfo> old_doi = SqlOperator.queryOneObject(db, DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where module_table_id = ? and version_date = ?", jobTableId, Constant._MAX_DATE_8);
            if (old_doi.isPresent()) {
                db.execute("delete from " + DmJobTableVersionInfo.TableName + " where module_table_id = ? and version_date = ?", jobTableId, etlDate);
                DmJobTableVersionInfo doi_ver = new DmJobTableVersionInfo();
                BeanUtil.copyProperties(old_doi.get(), doi_ver);
                doi_ver.setJobtab_version_id(PrimayKeyGener.getNextId());
                doi_ver.setVersion_date(etlDate);
                doi_ver.add(db);
            }
            db.execute("delete from " + DmJobTableInfo.TableName + " where module_table_id = ? and version_date = ?", jobTableId, Constant._MAX_DATE_8);
            db.execute("update " + DmJobTableInfo.TableName + " set" + " version_date = ? where module_table_id = ? and version_date <> ?", Constant._MAX_DATE_8, jobTableId, Constant._MAX_DATE_8);
        }
    }

    public void rollBack() {
        if (db != null) {
            db.rollback();
        }
    }

    public void commit() {
        if (db != null) {
            db.commit();
        }
    }

    @Override
    public void close() {
        if (db != null && db.isConnected()) {
            db.close();
        }
    }

    public void updateModuleVersion() {
        DmModuleTable runModule = SqlOperator.queryOneObject(db, DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where module_table_id = ?", moduleTableId).orElseThrow(() -> new BusinessException("find job failed."));
        DmModuleTableVersion versionModule = SqlOperator.queryOneObject(db, DmModuleTableVersion.class, "select * from " + DmModuleTableVersion.TableName + " where module_table_id = ? and version_date = ?", moduleTableId, etlDate).orElse(null);
        if (versionModule != null) {
            if (checkModuleTabAndField(runModule, versionModule)) {
                log.info(" module table fields changes, update begin ... moduleTableId: {}", moduleTableId);
                db.execute("delete from " + DmModuleTableFieldVersion.TableName + " where module_table_id = ? and version_date = ?", moduleTableId, etlDate);
                db.execute("delete from " + DmModuleTableVersion.TableName + " where module_table_id = ? and version_date = ?", moduleTableId, etlDate);
            } else {
                return;
            }
        }
        DmModuleTableVersion versionInfo = new DmModuleTableVersion();
        BeanUtils.copyProperties(runModule, versionInfo);
        versionInfo.setVersion_date(etlDate);
        versionInfo.setMtab_ver_id(PrimayKeyGener.getNextId());
        versionInfo.add(db);
        List<DmModuleTableFieldInfo> moduleFields = SqlOperator.queryList(db, DmModuleTableFieldInfo.class, "select * from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", moduleTableId);
        for (DmModuleTableFieldInfo moduleField : moduleFields) {
            DmModuleTableFieldVersion dmModuleTableFieldVersion = new DmModuleTableFieldVersion();
            BeanUtils.copyProperties(moduleField, dmModuleTableFieldVersion);
            dmModuleTableFieldVersion.setMtab_f_ver_id(PrimayKeyGener.getNextId());
            dmModuleTableFieldVersion.setVersion_date(etlDate);
            dmModuleTableFieldVersion.add(db);
        }
        db.commit();
    }

    private boolean checkModuleTabAndField(DmModuleTable runModule, DmModuleTableVersion versionModule) {
        boolean flag = true;
        if (versionModule.getModule_table_en_name() != null) {
            flag = versionModule.getModule_table_en_name().equals(runModule.getModule_table_en_name());
        }
        if (versionModule.getModule_table_cn_name() != null) {
            flag = versionModule.getModule_table_cn_name().equals(runModule.getModule_table_cn_name());
        }
        if (versionModule.getModule_table_life_cycle() != null) {
            flag = versionModule.getModule_table_life_cycle().equals(runModule.getModule_table_life_cycle());
        }
        if (versionModule.getEtl_date() != null) {
            flag = versionModule.getEtl_date().equals(runModule.getEtl_date());
        }
        if (versionModule.getSql_engine() != null) {
            flag = versionModule.getSql_engine().equals(runModule.getSql_engine());
        }
        if (versionModule.getStorage_type() != null) {
            flag = versionModule.getStorage_type().equals(runModule.getStorage_type());
        }
        if (versionModule.getTable_storage() != null) {
            flag = versionModule.getTable_storage().equals(runModule.getTable_storage());
        }
        if (versionModule.getPre_partition() != null) {
            flag = versionModule.getPre_partition().equals(runModule.getPre_partition());
        }
        if (versionModule.getRemark() != null) {
            flag = versionModule.getRemark().equals(runModule.getRemark());
        }
        if (versionModule.getModule_table_desc() != null) {
            flag = versionModule.getModule_table_desc().equals(runModule.getModule_table_desc());
        }
        if (flag) {
            List<DmModuleTableFieldInfo> moduleFields = SqlOperator.queryList(db, DmModuleTableFieldInfo.class, "select * from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", moduleTableId);
            List<DmModuleTableFieldVersion> versionFields = SqlOperator.queryList(db, DmModuleTableFieldVersion.class, "select * from " + DmModuleTableFieldVersion.TableName + " where module_table_id = ? and version_date = ?", moduleTableId, etlDate);
            if (moduleFields.size() != versionFields.size()) {
                return true;
            }
            for (DmModuleTableFieldInfo moduleField : moduleFields) {
                Long moduleFieldId = moduleField.getModule_field_id();
                List<DmModuleTableFieldVersion> nameEqualBeans = versionFields.stream().filter(dmModuleTableFieldVersion -> dmModuleTableFieldVersion.getModule_field_id().equals(moduleFieldId)).collect(Collectors.toList());
                if (nameEqualBeans.size() != 1) {
                    return true;
                }
                DmModuleTableFieldVersion nameEqualBean = nameEqualBeans.get(0);
                if (moduleField.getField_en_name() != null) {
                    flag = moduleField.getField_en_name().equals(nameEqualBean.getField_en_name());
                }
                if (moduleField.getField_cn_name() != null) {
                    flag = moduleField.getField_cn_name().equals(nameEqualBean.getField_cn_name());
                }
                if (moduleField.getField_type() != null) {
                    flag = moduleField.getField_type().equals(nameEqualBean.getField_type());
                }
                if (moduleField.getField_length() != null) {
                    flag = moduleField.getField_length().equals(nameEqualBean.getField_length());
                }
                if (moduleField.getField_seq() != null) {
                    flag = moduleField.getField_seq().equals(nameEqualBean.getField_seq());
                }
                if (moduleField.getRemark() != null) {
                    flag = moduleField.getRemark().equals(nameEqualBean.getRemark());
                }
                return !flag;
            }
        }
        return !flag;
    }
}
