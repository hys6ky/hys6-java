package hyren.serv6.g.enumerate;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;

@DocClass(desc = "", author = "dhw", createdate = "2020/4/2 15:21")
public enum OutType {

    FILE("file", "数据文件"), STREAM("stream", "数据流");

    private final String code;

    private final String value;

    OutType(String code, String value) {
        this.code = code;
        this.value = value;
    }

    public String getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }

    public static String ofValueByCode(String code) {
        for (OutType outType : OutType.values()) {
            if (outType.getCode().equals(code)) {
                return outType.value;
            }
        }
        return null;
    }

    public static OutType ofEnumByCode(String code) {
        for (OutType outType : OutType.values()) {
            if (outType.getCode().equals(code)) {
                return outType;
            }
        }
        return null;
    }

    public static boolean isOutType(String outType) {
        if (StringUtil.isNotBlank(outType) && (OutType.STREAM == OutType.ofEnumByCode(outType) || OutType.FILE == OutType.ofEnumByCode(outType))) {
            return true;
        } else {
            return false;
        }
    }
}
