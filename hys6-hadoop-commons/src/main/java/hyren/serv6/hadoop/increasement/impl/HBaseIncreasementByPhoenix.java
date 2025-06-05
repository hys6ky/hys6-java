package hyren.serv6.hadoop.increasement.impl;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.hadoop.increasement.HBaseIncreasement;
import hyren.serv6.commons.utils.agent.bean.TableBean;

public class HBaseIncreasementByPhoenix extends HBaseIncreasement {

    public HBaseIncreasementByPhoenix(TableBean tableBean, String hbase_name, String sysDate, String dsl_name, String hadoop_user_name, String platform, String prncipal_name, Long dsl_id, DatabaseWrapper db) {
        super(tableBean, hbase_name, sysDate, dsl_name, hadoop_user_name, platform, prncipal_name, dsl_id, db);
    }

    @Override
    public void calculateIncrement() {
    }

    @Override
    public void mergeIncrement() {
    }

    @Override
    public void append() {
    }

    @Override
    public void restore(String storageType) {
    }

    @Override
    public void replace() {
    }

    @Override
    public void incrementalDataZipper() {
    }
}
