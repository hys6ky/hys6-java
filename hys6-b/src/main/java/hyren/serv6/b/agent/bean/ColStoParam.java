package hyren.serv6.b.agent.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import hyren.serv6.base.entity.fdentity.ProEntity;
import org.springframework.stereotype.Component;
import java.util.Arrays;

@Component
@DocClass(desc = "", author = "WangZhengcheng")
public class ColStoParam extends ProEntity {

    private static final long serialVersionUID = -1666378328728519042L;

    @DocBean(name = "columnId", value = "", dataType = Long.class, required = false)
    private Long columnId;

    @DocBean(name = "dsladIds", value = "", dataType = Long[].class, required = false)
    private Long[] dsladIds;

    @DocBean(name = "csiNumber", value = "", dataType = Long.class, required = true)
    private Long csiNumber;

    public Long getColumnId() {
        return columnId;
    }

    public void setColumnId(Long columnId) {
        this.columnId = columnId;
    }

    public Long[] getDsladIds() {
        return dsladIds;
    }

    public void setDsladIds(Long[] dsladIds) {
        this.dsladIds = dsladIds;
    }

    public Long getCsiNumber() {
        return csiNumber;
    }

    public void setCsiNumber(Long csiNumber) {
        this.csiNumber = csiNumber;
    }

    @Override
    public String toString() {
        return "ColStoParam{" + "columnId=" + columnId + ", dsladIds=" + Arrays.toString(dsladIds) + ", csiNumber=" + csiNumber + '}';
    }
}
