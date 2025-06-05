package hyren.serv6.h.process.spark.loader.impl;

import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.h.process.args.HandleArgs;
import hyren.serv6.h.process.spark.loader.ISparkLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class AbsSparkLoader implements ISparkLoader {

    private static final long serialVersionUID = -7297378629883889594L;

    protected final SparkSession sparkSession;

    protected final Dataset<Row> rowDataset;

    protected final HandleArgs handleArgs;

    protected String tableName;

    protected String currentTempTable;

    protected String zipperTempTable;

    protected String etlDateWith8;

    protected String jobNameParam;

    public AbsSparkLoader(SparkSession sparkSession, Dataset<Row> rowDataset, HandleArgs handleArgs) {
        this.sparkSession = sparkSession;
        this.rowDataset = rowDataset;
        this.handleArgs = handleArgs;
        this.tableName = handleArgs.getTableName();
        this.currentTempTable = tableName + "_current";
        this.zipperTempTable = tableName + "_zipper";
        this.etlDateWith8 = handleArgs.getEtlDateWith8();
    }

    @Override
    public void append() {
        rowDataset.write().mode(SaveMode.Append).option("partitionOverwriteMode", "dynamic").format("parquet").insertInto(tableName);
    }

    @Override
    public void replace() {
        rowDataset.write().mode(SaveMode.Append).option("partitionOverwriteMode", "dynamic").format("parquet").insertInto(tableName);
    }

    @Override
    public void upSert() {
        throw new BusinessException("需要具体实现 ISparkLoader 接口的 upSert()");
    }

    @Override
    public void historyZipperFullLoading() {
        throw new BusinessException("需要具体实现 ISparkLoader 接口的 historyZipperFullLoading()");
    }

    @Override
    public void historyZipperIncrementLoading() {
        throw new BusinessException("需要具体实现 ISparkLoader 接口的 historyZipperIncrementLoading()");
    }

    @Override
    public void incrementalDataZipper() {
        throw new BusinessException("需要具体实现 ISparkLoader 接口的 incrementalDataZipper()");
    }
}
