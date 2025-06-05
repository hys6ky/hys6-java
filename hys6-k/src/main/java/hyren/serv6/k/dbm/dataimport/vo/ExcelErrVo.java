package hyren.serv6.k.dbm.dataimport.vo;

import lombok.Data;
import lombok.Getter;
import java.util.HashSet;
import java.util.Set;

public class ExcelErrVo {

    @Getter
    private String errMessage;

    private Set<String> errData;

    public ExcelErrVo() {
        errMessage = "";
        errData = new HashSet<>();
    }

    public String getErrMessage() {
        return errMessage;
    }

    public Set<String> getErrData() {
        return errData;
    }

    public void setErrMessage(String errMessage) {
        this.errMessage = this.errMessage + errMessage;
    }

    public void setErrData(String errData) {
        this.errData.add(errData);
    }

    public void setErrData(Set<String> errData) {
        this.errData = errData;
    }
}
