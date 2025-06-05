package hyren.serv6.h.process.spark.handle.impl;

import hyren.serv6.h.process.args.HiveHandleArgs;
import hyren.serv6.h.process.spark.loader.impl.HiveOnSparkLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveLoadLogicHandler extends AbsLoadLogicHandler {

    private static final long serialVersionUID = 2923180058248535297L;

    public HiveLoadLogicHandler(SparkSession sparkSession, Dataset<Row> rowDataset, HiveHandleArgs handleArgs) {
        super(sparkSession, rowDataset, handleArgs);
        this.sparkLoader = new HiveOnSparkLoader(sparkSession, rowDataset, handleArgs);
        this.sparkSession.catalog().setCurrentDatabase(handleArgs.getDatabase());
    }
}
