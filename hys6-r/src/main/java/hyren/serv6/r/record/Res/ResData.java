package hyren.serv6.r.record.Res;

import lombok.Data;

@Data
public class ResData {

    private Integer count;

    private Object data;

    private Object keyData;

    public ResData() {
    }

    public ResData(Integer count, Object data) {
        this.count = count;
        this.data = data;
    }
}
