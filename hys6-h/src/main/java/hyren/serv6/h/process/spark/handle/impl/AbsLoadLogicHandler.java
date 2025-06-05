package hyren.serv6.h.process.spark.handle.impl;

import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.h.process.args.HandleArgs;
import hyren.serv6.h.process.spark.handle.ILoadLogicHandler;
import hyren.serv6.h.process.spark.loader.ISparkLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AbsLoadLogicHandler implements ILoadLogicHandler {

    private static final long serialVersionUID = 4450472242048142066L;

    protected SparkSession sparkSession;

    protected Dataset<Row> rowDataset;

    protected HandleArgs handleArgs;

    protected ISparkLoader sparkLoader;

    protected AbsLoadLogicHandler(SparkSession sparkSession, Dataset<Row> rowDataset, HandleArgs handleArgs) {
        this.sparkSession = sparkSession;
        this.rowDataset = rowDataset;
        this.handleArgs = handleArgs;
    }

    public void handleTempTable() {
        sparkLoader.replace();
    }

    public void handleModelTable() {
        StorageType storageType = handleArgs.getStorageType();
        switch(storageType) {
            case ZhuiJia:
                sparkLoader.append();
                break;
            case TiHuan:
                sparkLoader.replace();
                break;
            case UpSet:
                sparkLoader.upSert();
                break;
            case QuanLiang:
                sparkLoader.historyZipperFullLoading();
                break;
            case LiShiLaLian:
                sparkLoader.historyZipperIncrementLoading();
                break;
            case ZengLiang:
                sparkLoader.incrementalDataZipper();
                break;
            default:
                throw new BusinessException("数据处理加载时,未知的进数方式! " + storageType.getValue());
        }
    }

    @Override
    public HandleArgs getHandleArgs() {
        return handleArgs;
    }
}
