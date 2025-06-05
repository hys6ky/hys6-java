package hyren.serv6.g.enumerate;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;

@DocClass(desc = "", author = "dhw", createdate = "2020/4/2 15:21")
public enum DataType {

    json("json", "json"), csv("csv", "csv");

    private final String code;

    private final String value;

    DataType(String code, String value) {
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
        for (DataType dataType : DataType.values()) {
            if (dataType.getCode().equals(code)) {
                return dataType.value;
            }
        }
        return null;
    }

    public static DataType ofEnumByCode(String code) {
        for (DataType dataType : DataType.values()) {
            if (dataType.getCode().equals(code)) {
                return dataType;
            }
        }
        return null;
    }

    public static boolean isDataType(String dataType) {
        if (StringUtil.isNotBlank(dataType) && (DataType.json == DataType.ofEnumByCode(dataType) || DataType.csv == DataType.ofEnumByCode(dataType))) {
            return true;
        } else {
            return false;
        }
    }
}
