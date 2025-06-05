package hyren.serv6.b.agent.bean;

import fd.ng.core.annotation.DocClass;

@DocClass(desc = "", author = "WangZhengcheng")
public class CollTbConfParam {

    private String collColumnString;

    public String getCollColumnString() {
        return collColumnString;
    }

    public void setCollColumnString(String collColumnString) {
        this.collColumnString = collColumnString;
    }

    @Override
    public String toString() {
        return "CollTbConfParam{" + "collColumnString='" + collColumnString + '\'' + '}';
    }
}
