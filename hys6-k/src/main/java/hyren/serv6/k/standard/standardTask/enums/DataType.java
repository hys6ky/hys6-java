package hyren.serv6.k.standard.standardTask.enums;

import hyren.daos.base.exception.SystemBusinessException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum DataType {

    DINGCHANG("001", Collections.singletonList("CHAR"), "", "数据类型"),
    BIANCHANG("002", Arrays.asList("VARCHAR", "STRING", "TEXT"), "", "数据类型"),
    XIAOSHU_FLOAT("003", Collections.singletonList("FLOAT"), "", "数据类型"),
    XIAOSHU_DOUBLE("004", Collections.singletonList("DOUBLE"), "", "数据类型"),
    XIAOSHU_DECIMAL("005", Arrays.asList("DECIMAL", "NUMERIC"), "", "数据类型"),
    RIQI("006", Collections.singletonList("DATE"), "", "数据类型"),
    SHIJIAN("007", Collections.singletonList("TIME"), "", "数据类型"),
    RIQI_SHIJIAN("008", Arrays.asList("DATETIME", "TIMESTAMP"), "", "数据类型"),
    ZHENGSHU("009", Arrays.asList("INT", "INTEGER", "BIGINT", "LONG", "SMALLINT", "TINYINT"), "", "数据类型"),
    DUIXIANG("010", Collections.singletonList("BLOB"), "", "数据类型");

    private final String code;

    private final List<String> value;

    private final String catCode;

    private final String catValue;

    DataType(String code, List<String> value, String catCode, String catValue) {
        this.code = code;
        this.value = value;
        this.catCode = catCode;
        this.catValue = catValue;
    }

    public String getCode() {
        return code;
    }

    public List<String> getValue() {
        return value;
    }

    public String getCatCode() {
        return catCode;
    }

    public String getCatValue() {
        return catValue;
    }

    public static final String CodeName = "DataType";

    public static List<String> ofValueByCode(String code) {
        for (DataType typeCode : DataType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[ConfigType:数据类型]");
    }

    public static DataType ofEnumByCode(String code) {
        for (DataType typeCode : DataType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[ConfigType:数据类型]");
    }

    public static String getCodeByValue(String value) {
        for (DataType typeCode : DataType.values()) {
            if (typeCode.getValue().contains(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[ConfigType:数据类型]");
    }

    public static String ofCatValue() {
        return DataType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return DataType.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
