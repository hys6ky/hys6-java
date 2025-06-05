package hyren.serv6.h.process.loader.business.impl;

import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.h.process.loader.ILoader;
import hyren.serv6.h.process.loader.business.AbsBusinessImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessBusinessImpl extends AbsBusinessImpl {

    public ProcessBusinessImpl(ILoader loader) {
        this.loader = loader;
        this.processJobTableConfBean = loader.getProcessJobTableConfBean();
    }

    @Override
    public void handleModuleTableJob() {
        String loaderName = loader.getClass().getSimpleName();
        log.info("处理模型表作业,开始计算并导入数据，导入类型为: " + loaderName);
        StorageType storageType = StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type());
        loader.init();
        loader.restore();
        try {
            String tarTableName = processJobTableConfBean.getTarTableName();
            log.info("---- 表 [ " + tarTableName + " ] ---- 进数类型: " + storageType.getValue());
            IsFlag isTempFlag = processJobTableConfBean.getIsTempFlag();
            if (storageType == StorageType.TiHuan) {
                loader.replace();
            } else if (storageType == StorageType.ZhuiJia) {
                loader.append();
            } else if (storageType == StorageType.QuanLiang) {
                loader.historyZipperFullLoading();
            } else if (storageType == StorageType.LiShiLaLian) {
                loader.historyZipperIncrementLoading();
            } else if (storageType == StorageType.UpSet) {
                loader.upSert();
            } else if (storageType == StorageType.ZengLiang) {
                throw new AppSystemException("进数方式: " + storageType.getValue() + " 暂不支持! ");
            } else {
                throw new AppSystemException("无效的进数方式: " + storageType);
            }
        } catch (Exception e) {
            try {
                log.error("加工作业表执行失败,执行回滚操作");
                loader.restore();
                loader.handleException();
            } catch (Exception warn) {
                log.error("加工作业表作业回滚异常: ", e);
            }
        }
        loader.clean();
        loader.close();
    }

    @Override
    public void handleTempTableJob() {
        super.handleTempTableJob();
    }
}
