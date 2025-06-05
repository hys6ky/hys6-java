package hyren.serv6.h.process.loader.chooser;

import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.loader.ILoader;
import hyren.serv6.h.process.loader.impl.HiveLoader;
import org.springframework.stereotype.Service;

@Service
public class HadoopLoaderChooserImpl {

    public ILoader choiceLoader(ProcessJobTableConfBean processJobTableConfBean) {
        Store_type storeType = Store_type.ofEnumByCode(processJobTableConfBean.getDataStoreLayer().getStore_type());
        ILoader iLoader;
        switch(storeType) {
            case HIVE:
                iLoader = new HiveLoader(processJobTableConfBean);
                break;
            case HBASE:
                throw new BusinessException("暂未实现的存储类型! HBASE");
            case CARBONDATA:
                throw new BusinessException("暂未实现的存储类型! CARBONDATA");
            default:
                throw new BusinessException("无法处理类型 storeType: " + storeType.getValue());
        }
        return iLoader;
    }
}
