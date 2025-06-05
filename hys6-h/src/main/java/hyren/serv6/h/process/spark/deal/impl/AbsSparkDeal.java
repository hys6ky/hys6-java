package hyren.serv6.h.process.spark.deal.impl;

import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.spark.builder.ISparkSessionBuilder;
import hyren.serv6.h.process.spark.builder.SparkSessionBuilder;
import hyren.serv6.h.process.spark.dataset.DataSetProcesser;
import hyren.serv6.h.process.spark.deal.ISparkDeal;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import java.util.List;
import java.util.Map;

@Slf4j
public class AbsSparkDeal implements ISparkDeal {

    private final SparkSession sparkSession;

    private Dataset<Row> dataset;

    private final ProcessJobTableConfBean processJobTableConfBean;

    private final DataSetProcesser dataSetProcesser;

    private boolean dataSetAlreadyProcessed = false;

    public AbsSparkDeal(ProcessJobTableConfBean processJobTableConfBean, DataSetProcesser dataSetProcesser) {
        ISparkSessionBuilder sparkSessionBuilder = new SparkSessionBuilder(processJobTableConfBean);
        this.sparkSession = sparkSessionBuilder.build();
        this.processJobTableConfBean = processJobTableConfBean;
        this.dataSetProcesser = dataSetProcesser;
    }

    @Override
    public Dataset<Row> getDataset() throws Exception {
        return dataSetAlreadyProcessed ? dataset : getProcessedDataSet();
    }

    private Dataset<Row> getProcessedDataSet() throws Exception {
        dataset = dataframeFromSql();
        dataset = processDataSet(dataset);
        dataSetAlreadyProcessed = true;
        return dataset;
    }

    private Dataset<Row> processDataSet(Dataset<Row> dataSet) throws Exception {
        return dataSetProcesser == null ? dataSet : dataSetProcesser.process(dataSet);
    }

    @Override
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    @Override
    public void close() {
        if (sparkSession != null) {
            try {
                sparkSession.close();
            } catch (Exception e) {
                throw new AppSystemException(" Closing SparkSession failed! " + e);
            }
        }
    }

    private Dataset<Row> dataframeFromSql() throws Exception {
        Catalog catalog = sparkSession.catalog();
        Map<String, List<LayerBean>> layerBeansByTableMap = processJobTableConfBean.getLayerBeansByTableMap();
        if (layerBeansByTableMap.isEmpty()) {
            throw new Exception("Spark processed processing jobs, obtained empty table storage layer Map mapping" + " information! ");
        }
        try {
            layerBeansByTableMap.forEach((tableName, layerBeans) -> {
                if (layerBeans.isEmpty()) {
                    log.error("The storage layer information corresponding to the table is empty! " + "tableName: " + tableName);
                } else {
                    Map<String, String> layerAttr = layerBeans.get(0).getLayerAttr();
                    if (needCreateTempView(layerBeans)) {
                        createJdbcTempView(tableName, layerAttr);
                    }
                    String database_name = layerAttr.get(StorageTypeKey.database_name);
                    if (catalog.databaseExists(database_name)) {
                        log.info("catalog.setCurrentDatabase(database_name): " + database_name);
                        catalog.setCurrentDatabase(database_name);
                    }
                }
            });
        } catch (Exception e) {
            throw new Exception("An exception occurred while processing the table used for processing operations! e: " + e);
        }
        log.info("SparkSession final execute sql: {}", processJobTableConfBean.getCompleteSql());
        return sparkSession.sql(processJobTableConfBean.getCompleteSql());
    }

    private static boolean needCreateTempView(List<LayerBean> layerByTable) {
        for (LayerBean layerBean : layerByTable) {
            if (Store_type.HIVE.getCode().equals(layerBean.getStore_type()) || Store_type.HBASE.getCode().equals(layerBean.getStore_type()) || Store_type.CARBONDATA.getCode().equals(layerBean.getStore_type())) {
                return false;
            }
        }
        return true;
    }

    private static boolean validTableLayer(List<LayerBean> layerByTable) {
        if (layerByTable == null)
            return false;
        for (LayerBean layerBean : layerByTable) {
            if (Store_type.HIVE.getCode().equals(layerBean.getStore_type()) || Store_type.HBASE.getCode().equals(layerBean.getStore_type()) || Store_type.DATABASE.getCode().equals(layerBean.getStore_type()) || Store_type.CARBONDATA.getCode().equals(layerBean.getStore_type())) {
                return true;
            }
        }
        return false;
    }

    private void createJdbcTempView(String tableName, Map<String, String> layerAttr) {
        sparkSession.read().format("jdbc").option("driver", layerAttr.get(StorageTypeKey.database_driver)).option("url", layerAttr.get(StorageTypeKey.jdbc_url)).option("dbtable", tableName).option("user", layerAttr.get(StorageTypeKey.user_name)).option("password", layerAttr.get(StorageTypeKey.database_pwd)).load().createOrReplaceTempView(tableName);
    }
}
