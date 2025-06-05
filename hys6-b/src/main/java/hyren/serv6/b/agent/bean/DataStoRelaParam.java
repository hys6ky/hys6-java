package hyren.serv6.b.agent.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.fdentity.ProEntity;
import org.springframework.stereotype.Component;
import java.util.Arrays;

@Component
@DocClass(desc = "", author = "WangZhengcheng")
public class DataStoRelaParam extends ProEntity {

    private static final long serialVersionUID = 321677610043865203L;

    @DocBean(name = "tableId", value = "", dataType = Long.class, required = false)
    private Long tableId;

    @DocBean(name = "dslIds", value = "", dataType = Long[].class, required = false)
    private Long[] dslIds;

    @DocBean(name = "hyren_name", value = "", dataType = String.class)
    private String hyren_name;

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public Long[] getDslIds() {
        return dslIds;
    }

    public void setDslIds(Long[] dslIds) {
        this.dslIds = dslIds;
    }

    public void setHyren_name(String hyren_name) {
        this.hyren_name = hyren_name;
    }

    public String getHyren_name() {
        return hyren_name;
    }

    @Override
    public String toString() {
        return "DataStoRelaParam{" + "tableId=" + tableId + ", dslIds=" + Arrays.toString(dslIds) + '}';
    }
}
