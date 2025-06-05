package hyren.serv6.commons.collection.bean;

import fd.ng.core.annotation.DocBean;
import lombok.Getter;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
public class LayerBean implements Serializable {

    private static final long serialVersionUID = 321566870187324L;

    @DocBean(name = "dsl_id", value = "", dataType = Long.class)
    private Long dsl_id;

    @DocBean(name = "dsl_name", value = "", dataType = String.class)
    private String dsl_name;

    @DocBean(name = "store_type", value = "", dataType = String.class)
    private String store_type;

    @DocBean(name = "dst", value = "", dataType = String.class, required = false)
    private String dst;

    private Map<String, String> layerAttr;

    public void setLayerAttr(Map<String, String> layerAttr) {
        this.layerAttr = layerAttr;
    }

    public void setTableNameList(Set<String> tableNameList) {
        this.tableNameList = tableNameList;
    }

    @DocBean(name = "tableNameList", value = "", dataType = List.class, required = false)
    private Set<String> tableNameList;

    public void setDst(String dst) {
        this.dst = dst;
    }

    public void setDsl_id(Long dsl_id) {
        this.dsl_id = dsl_id;
    }

    public void setDsl_id(String dsl_id) {
        if (!fd.ng.core.utils.StringUtil.isEmpty(dsl_id)) {
            this.dsl_id = new Long(dsl_id);
        }
    }

    public void setDsl_name(String dsl_name) {
        this.dsl_name = dsl_name;
    }

    public void setStore_type(String store_type) {
        this.store_type = store_type;
    }
}
