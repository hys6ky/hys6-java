package hyren.serv6.agent.job.biz.core;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public abstract class AbstractJobStage implements JobStageInterface {

    private JobStageInterface nextStage;

    @Method(desc = "", logicStep = "")
    @Param(name = "stage", desc = "", range = "")
    @Override
    public void setNextStage(JobStageInterface stage) {
        this.nextStage = stage;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public JobStageInterface getNextStage() {
        return nextStage;
    }

    public static void backupToDayTable(String todayTableName, DatabaseWrapper db) {
        String backupTableName = TableNameUtil.getBackupTableNameSuffixB(todayTableName);
        if (db.isExistTable(todayTableName)) {
            if (db.isExistTable(backupTableName)) {
                if (db.getDbtype() == Dbtype.KINGBASE) {
                    db.execute("DROP TABLE " + db.getDatabaseName() + '.' + backupTableName);
                } else {
                    db.execute("DROP TABLE " + backupTableName);
                }
            }
            db.execute(db.getDbtype().ofRenameSql(todayTableName, backupTableName, db));
        }
    }

    public static void dropToDayTable(String todayTableName, DatabaseWrapper db) {
        if (db.isExistTable(todayTableName)) {
            if (db.getDbtype() == Dbtype.KINGBASE) {
                db.execute("DROP TABLE " + db.getDatabaseName() + '.' + todayTableName);
            } else {
                db.execute("DROP TABLE " + todayTableName);
            }
        }
    }

    public static void backupToDayTable(String todayTableName) {
        ClassBase.HbaseInstance().backupToDayTable(todayTableName);
    }

    public static void backupPastTable(CollectTableBean collectTableBean, DatabaseWrapper db) {
        Long storage_time = collectTableBean.getStorage_time();
        String storageTableName = collectTableBean.getStorage_table_name();
        String underline1bTableName = TableNameUtil.getUnderline1bTableName(storageTableName);
        if (collectTableBean.getEtlDate().equals(collectTableBean.getStorage_date()) || storage_time <= 1) {
            if (db.isExistTable(underline1bTableName)) {
                if (db.getDbtype() == Dbtype.KINGBASE) {
                    db.execute("DROP TABLE " + db.getDatabaseName() + '.' + underline1bTableName);
                } else {
                    db.execute("DROP TABLE " + underline1bTableName);
                }
            }
        } else {
            for (long i = storage_time; i > 1; i--) {
                if (db.isExistTable(TableNameUtil.getSpliceTableName(storageTableName, i))) {
                    if (i == storage_time) {
                        if (db.getDbtype() == Dbtype.KINGBASE) {
                            db.execute("DROP TABLE " + db.getDatabaseName() + '.' + TableNameUtil.getSpliceTableName(storageTableName, i));
                        } else {
                            db.execute("DROP TABLE " + TableNameUtil.getSpliceTableName(storageTableName, i));
                        }
                    } else {
                        if (db.isExistTable(TableNameUtil.getSpliceTableName(storageTableName, (i + 1)))) {
                            if (db.getDbtype() == Dbtype.KINGBASE) {
                                db.execute("DROP TABLE " + db.getDatabaseName() + '.' + TableNameUtil.getSpliceTableName(storageTableName, (i + 1)));
                            } else {
                                db.execute("DROP TABLE " + TableNameUtil.getSpliceTableName(storageTableName, (i + 1)));
                            }
                        }
                        db.execute(db.getDbtype().ofRenameSql(TableNameUtil.getSpliceTableName(storageTableName, i), TableNameUtil.getSpliceTableName(storageTableName, i + 1), db));
                    }
                }
            }
            if (db.isExistTable(underline1bTableName)) {
                if (db.isExistTable(TableNameUtil.getSpliceTableName(storageTableName, 2))) {
                    if (db.getDbtype() == Dbtype.KINGBASE) {
                        db.execute("DROP TABLE " + db.getDatabaseName() + '.' + TableNameUtil.getSpliceTableName(storageTableName, 2));
                    } else {
                        db.execute("DROP TABLE " + TableNameUtil.getSpliceTableName(storageTableName, 2));
                    }
                }
                db.execute(db.getDbtype().ofRenameSql(underline1bTableName, TableNameUtil.getSpliceTableName(storageTableName, 2), db));
            }
        }
    }

    public static void backupPastTable(CollectTableBean collectTableBean) {
        Long storage_time = collectTableBean.getStorage_time();
        String storageTableName = collectTableBean.getStorage_table_name();
        String etlDate = collectTableBean.getEtlDate();
        String storageDate = collectTableBean.getStorage_date();
        ClassBase.HbaseInstance().backupPastTable(storage_time, storageTableName, etlDate, storageDate);
    }

    public static void recoverBackupToDayTable(String todayTableName, DatabaseWrapper db) {
        String backupTableName = TableNameUtil.getBackupTableNameSuffixB(todayTableName);
        if (db.isExistTable(backupTableName)) {
            if (db.isExistTable(todayTableName)) {
                if (db.getDbtype() == Dbtype.KINGBASE) {
                    db.execute("DROP TABLE " + db.getDatabaseName() + '.' + todayTableName);
                } else {
                    db.execute("DROP TABLE " + todayTableName);
                }
            }
            db.execute(db.getDbtype().ofRenameSql(backupTableName, todayTableName, db));
        }
    }

    public static void recoverBackupToDayTable(String todayTableName) {
        ClassBase.HbaseInstance().recoverBackupToDayTable(todayTableName);
    }
}
