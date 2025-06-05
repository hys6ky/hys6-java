package hyren.serv6.h.process.spark.handle.impl;

import hyren.serv6.h.process.args.DatabaseHandleArgs;
import hyren.serv6.h.process.spark.loader.impl.DatabaseOnSparkLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Properties;

public class DatabaseLoadLogicHandler extends AbsLoadLogicHandler {

    private static final long serialVersionUID = 6987141666877238946L;

    private final Properties connProperties = new Properties();

    public DatabaseLoadLogicHandler(SparkSession sparkSession, Dataset<Row> rowDataset, DatabaseHandleArgs handleArgs) {
        super(sparkSession, rowDataset, handleArgs);
        this.sparkLoader = new DatabaseOnSparkLoader(sparkSession, rowDataset, handleArgs);
    }
}
