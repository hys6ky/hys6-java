package hyren.serv6.m.main.entity;

import lombok.Data;

@Data
public class ColumnMetaVo extends fd.ng.db.meta.ColumnMeta {

    private String ordPosition;

    public String getOrdPosition() {
        return ordPosition;
    }

    public void setOrdPosition(String ordPosition) {
        this.ordPosition = ordPosition;
    }
}
