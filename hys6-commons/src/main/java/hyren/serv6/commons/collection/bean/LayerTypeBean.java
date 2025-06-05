package hyren.serv6.commons.collection.bean;

import java.util.List;

public class LayerTypeBean {

    public enum ConnType {

        oneJdbc, moreJdbc, oneOther, moreOther
    }

    private ConnType connType;

    private LayerBean layerBean;

    private List<LayerBean> layerBeanList;

    public ConnType getConnType() {
        return connType;
    }

    public void setConnType(ConnType connType) {
        this.connType = connType;
    }

    public List<LayerBean> getLayerBeanList() {
        return layerBeanList;
    }

    public void setLayerBeanList(List<LayerBean> layerBeanList) {
        this.layerBeanList = layerBeanList;
    }

    public LayerBean getLayerBean() {
        return layerBean;
    }

    public void setLayerBean(LayerBean layerBean) {
        this.layerBean = layerBean;
    }
}
