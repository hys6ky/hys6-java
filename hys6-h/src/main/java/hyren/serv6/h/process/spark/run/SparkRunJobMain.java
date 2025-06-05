package hyren.serv6.h.process.spark.run;

import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.h.process.args.DatabaseHandleArgs;
import hyren.serv6.h.process.args.HandleArgs;
import hyren.serv6.h.process.args.HiveHandleArgs;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.spark.deal.ISparkDeal;
import hyren.serv6.h.process.spark.deal.impl.HyrenSparkDealImpl;
import hyren.serv6.h.process.spark.handle.ILoadLogicHandler;
import hyren.serv6.h.process.spark.handle.impl.DatabaseLoadLogicHandler;
import hyren.serv6.h.process.spark.handle.impl.HiveLoadLogicHandler;
import hyren.serv6.h.process.utils.ProcessTableConfBeanUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkRunJobMain {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 2) {
            log.info("请按照规定的格式传入参数,必须参数不能为空");
            log.info("必须参数: 参数1: 模型表id_作业表id;参数2: 运行作业需要的 HandleArgs 对象 String 信息");
            System.exit(-1);
        }
        String moduleTableId_JobTableId = args[0];
        String handleArgs = args[1];
        ProcessJobTableConfBean processJobTableConfBean = ProcessTableConfBeanUtil.parsingJobSerializeFile(moduleTableId_JobTableId);
        try (ISparkDeal sparkDeal = new HyrenSparkDealImpl(processJobTableConfBean)) {
            SparkSession sparkSession = sparkDeal.getSparkSession();
            Store_type storeType = HandleArgs.handleStringToHandleClass(handleArgs, HandleArgs.class).getStoreType();
            ILoadLogicHandler loadLogicHandler;
            switch(storeType) {
                case HIVE:
                    loadLogicHandler = new HiveLoadLogicHandler(sparkSession, sparkDeal.getDataset(), (HiveHandleArgs) HandleArgs.handleStringToHandleClass(handleArgs, HiveHandleArgs.class));
                    break;
                case DATABASE:
                    loadLogicHandler = new DatabaseLoadLogicHandler(sparkSession, sparkDeal.getDataset(), (DatabaseHandleArgs) HandleArgs.handleStringToHandleClass(handleArgs, DatabaseHandleArgs.class));
                    break;
                case HBASE:
                case SOLR:
                case CARBONDATA:
                    throw new Exception("暂未实现处理类型 storeType: " + storeType.getValue());
                default:
                    throw new Exception("无法处理类型: " + storeType.getValue());
            }
            executeJob(loadLogicHandler);
        }
    }

    private static void executeJob(final ILoadLogicHandler handler) throws Exception {
        IsFlag isTempTableFlag = handler.getHandleArgs().getIsTempFlag();
        if (isTempTableFlag == IsFlag.Shi) {
            handler.handleTempTable();
        } else if (isTempTableFlag == IsFlag.Fou) {
            handler.handleModelTable();
        } else {
            throw new Exception("不合法的表标记信息! " + isTempTableFlag.getValue());
        }
    }
}
