package hyren.serv6.h.process.spark.dataset.impl;

import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.spark.dataset.AbsDataSetProcesser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HyrenDataSetProcesser extends AbsDataSetProcesser {

    public HyrenDataSetProcesser(ProcessJobTableConfBean processJobTableConfBean) {
        super(processJobTableConfBean);
    }

    @Override
    public Dataset<Row> process(Dataset<Row> dataSet) throws Exception {
        return super.process(dataSet);
    }
}
