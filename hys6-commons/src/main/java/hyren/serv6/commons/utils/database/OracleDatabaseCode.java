package hyren.serv6.commons.utils.database;

import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.exception.AppSystemException;

public enum OracleDatabaseCode {

    UTF_8(DataBaseCode.UTF_8.getCode(), "UTF8", "Oracle字符集"), GBK(DataBaseCode.GBK.getCode(), "ZHS16GBK", "Oracle字符集"), UTF_16(DataBaseCode.UTF_16.getCode(), "AL16UTF16", "Oracle字符集"), GB2312(DataBaseCode.GB2312.getCode(), "ZHS16GBK", "Oracle字符集"), ISO_8859_1(DataBaseCode.ISO_8859_1.getCode(), "ZHS16GBK", "Oracle字符集");

    private final String code;

    private final String value;

    private final String catValue;

    OracleDatabaseCode(String code, String value, String catValue) {
        this.code = code;
        this.value = value;
        this.catValue = catValue;
    }

    public String getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }

    public String getCatValue() {
        return catValue;
    }

    public static final String CodeName = "OracleDatabaseCode";

    public static String ofValueByCode(String code) {
        for (OracleDatabaseCode typeCode : OracleDatabaseCode.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new AppSystemException("根据[" + code + "]没有找到对应的代码项[OracleDatabaseCode:采集编码]");
    }

    public static OracleDatabaseCode ofEnumByCode(String code) {
        for (OracleDatabaseCode typeCode : OracleDatabaseCode.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new AppSystemException("根据[" + code + "]没有找到对应的代码项[OracleDatabaseCode:采集编码]");
    }

    public static String getCodeByValue(String value) {
        for (OracleDatabaseCode typeCode : OracleDatabaseCode.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new AppSystemException("根据[" + value + "]没有找到对应的代码项[OracleDatabaseCode:采集编码]");
    }

    public static String ofCatValue() {
        return OracleDatabaseCode.values()[0].getCatValue();
    }

    @Override
    public String toString() {
        throw new AppSystemException("There's no need for you to !");
    }
}
