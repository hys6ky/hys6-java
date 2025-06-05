package hyren.serv6.m.util.dbConf.bean;

import fd.ng.core.annotation.DocClass;

@DocClass(desc = "", author = "WangZhengcheng")
public class DBConnectionProp {

    private String urlPrefix;

    private String ipPlaceholder;

    private String portPlaceholder;

    private String urlSuffix;

    private String dbDriver;

    public String getDbDriver() {
        return dbDriver;
    }

    public void setDbDriver(String dbDriver) {
        this.dbDriver = dbDriver;
    }

    public String getUrlPrefix() {
        return urlPrefix;
    }

    public void setUrlPrefix(String urlPrefix) {
        this.urlPrefix = urlPrefix;
    }

    public String getIpPlaceholder() {
        return ipPlaceholder;
    }

    public void setIpPlaceholder(String ipPlaceholder) {
        this.ipPlaceholder = ipPlaceholder;
    }

    public String getPortPlaceholder() {
        return portPlaceholder;
    }

    public void setPortPlaceholder(String portPlaceholder) {
        this.portPlaceholder = portPlaceholder;
    }

    public String getUrlSuffix() {
        return urlSuffix;
    }

    public void setUrlSuffix(String urlSuffix) {
        this.urlSuffix = urlSuffix;
    }

    @Override
    public String toString() {
        return "URLTemplate{" + "urlPrefix='" + urlPrefix + '\'' + ", ipPlaceholder='" + ipPlaceholder + '\'' + ", portPlaceholder='" + portPlaceholder + '\'' + ", urlSuffix='" + urlSuffix + '\'' + '}';
    }
}
