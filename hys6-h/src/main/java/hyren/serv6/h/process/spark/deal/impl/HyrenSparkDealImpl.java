package hyren.serv6.h.process.spark.deal.impl;

import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.spark.dataset.impl.HyrenDataSetProcesser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HyrenSparkDealImpl extends AbsSparkDeal {

    public HyrenSparkDealImpl(ProcessJobTableConfBean processJobTableConfBean) {
        super(processJobTableConfBean, new HyrenDataSetProcesser(processJobTableConfBean));
    }

    @Override
    public Dataset<Row> getDataset() throws Exception {
        return super.getDataset();
    }

    @Override
    public SparkSession getSparkSession() {
        return super.getSparkSession();
    }

    @Override
    public void close() {
        super.close();
    }
}
