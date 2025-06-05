package hyren.serv6.r.api.service;

import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import org.springframework.stereotype.Service;

@Service
public class DataLayerServiceImpl {

    public LayerBean getTableLayerByDslId(Long dslId) {
        LayerBean layerBean = ProcessingData.getLayerBean(dslId, Dbo.db());
        if (layerBean.getDsl_id() == null) {
            throw new BusinessException("获取存储层数据信息的SQL执行失败");
        } else {
            return layerBean;
        }
    }
}
