package hyren.serv6.h.process.spark.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataSetProcesser {

    Dataset<Row> process(Dataset<Row> dataSet) throws Exception;
}
