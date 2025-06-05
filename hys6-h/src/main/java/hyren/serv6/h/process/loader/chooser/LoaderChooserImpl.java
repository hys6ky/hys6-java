package hyren.serv6.h.process.loader.chooser;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.collection.bean.LayerTypeBean;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.loader.ILoader;
import hyren.serv6.h.process.loader.ILoaderChooser;
import hyren.serv6.h.process.loader.impl.DatabaseLoader;
import hyren.serv6.h.process.loader.impl.SameDatabaseLoader;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
public class LoaderChooserImpl implements ILoaderChooser {

    public ILoader choiceLoader(ProcessJobTableConfBean processJobTableConfBean) {
        Store_type storeType = Store_type.ofEnumByCode(processJobTableConfBean.getDataStoreLayer().getStore_type());
        ILoader iLoader;
        switch(storeType) {
            case DATABASE:
                if (isSameJdbc(processJobTableConfBean)) {
                    iLoader = new SameDatabaseLoader(processJobTableConfBean);
                } else {
                    iLoader = new DatabaseLoader(processJobTableConfBean);
                }
                break;
            case HIVE:
            case HBASE:
            case CARBONDATA:
                try {
                    iLoader = new HadoopLoaderChooserImpl().choiceLoader(processJobTableConfBean);
                } catch (Exception e) {
                    throw new AppSystemException("创建加工作业执行器对象发生异常! e: {}", e);
                }
                break;
            case SOLR:
                throw new BusinessException("暂未实现处理类型 storeType: " + storeType.getValue());
            default:
                throw new AppSystemException("无法处理类型 storeType: " + storeType.getValue());
        }
        return iLoader;
    }

    private static boolean isSameJdbc(ProcessJobTableConfBean processJobTableConfBean) {
        String sql = processJobTableConfBean.getBeforeReplaceSql();
        List<String> listTable = DruidParseQuerySql.parseSqlTableToList(sql);
        String tarTableName = processJobTableConfBean.getTarTableName();
        try (DatabaseWrapper db = new DatabaseWrapper.Builder().showsql(false).create()) {
            List<LayerBean> layerByTable = ProcessingData.getLayerByTable(tarTableName, db);
            if (layerByTable.isEmpty()) {
                throw new AppSystemException("无法获取表 " + tarTableName + " 的存储层");
            }
            LayerTypeBean allTableIsLayer = ProcessingData.getAllTableIsLayer(listTable, db);
            return allTableIsLayer.getConnType().equals(LayerTypeBean.ConnType.oneJdbc);
        }
    }
}
