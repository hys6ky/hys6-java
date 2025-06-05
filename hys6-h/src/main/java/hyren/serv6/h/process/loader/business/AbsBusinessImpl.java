package hyren.serv6.h.process.loader.business;

import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import hyren.serv6.h.process.loader.IBusiness;
import hyren.serv6.h.process.loader.ILoader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbsBusinessImpl implements IBusiness {

    protected ILoader loader;

    protected ProcessJobTableConfBean processJobTableConfBean;

    @Override
    public void handleModuleTableJob() {
        throw new BusinessException("请具体实现业务加载 IBusiness 接口!");
    }

    @Override
    public void handleTempTableJob() {
        String loaderName = loader.getClass().getSimpleName();
        log.info("处理临时表作业,开始计算并导入数据，导入类型为: " + loaderName);
        StorageType storageType = StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type());
        loader.init();
        loader.restore();
        String tarTableName = processJobTableConfBean.getTarTableName();
        log.info("---- 表 [ " + tarTableName + " ] ---- 进数类型: " + storageType.getValue());
        loader.replace();
        loader.clean();
        loader.close();
    }
}
