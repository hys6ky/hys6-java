package hyren.serv6.base.datatree.background.bean;

import fd.ng.core.annotation.DocClass;
import lombok.Getter;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/3/16 0016 下午 12:39")
public class TreeConf {

    private Boolean isShowFileCollection = Boolean.FALSE;

    @Getter
    private String isIntoHBase = "";

    private Boolean isShowDCLRealtime = Boolean.FALSE;

    public Boolean getShowFileCollection() {
        return isShowFileCollection;
    }

    public void setShowFileCollection(Boolean showFileCollection) {
        isShowFileCollection = showFileCollection;
    }

    public void setIsIntoHBase(String isIntoHBase) {
        this.isIntoHBase = isIntoHBase;
    }

    public Boolean getShowDCLRealtime() {
        return isShowDCLRealtime;
    }

    public void setShowDCLRealtime(Boolean showDCLRealtime) {
        isShowDCLRealtime = showDCLRealtime;
    }
}
