package hyren.serv6.h.process.spark.deal;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.Closeable;

public interface ISparkDeal extends Closeable {

    Dataset<Row> getDataset() throws Exception;

    SparkSession getSparkSession();
}
