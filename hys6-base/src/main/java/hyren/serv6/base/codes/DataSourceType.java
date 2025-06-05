package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum DataSourceType {

    ISL("ISL", "贴源层_01", "63", "数据源类型"),
    DCL("DCL", "贴源层", "63", "数据源类型"),
    DPL("DPL", "加工层-废弃", "63", "数据源类型"),
    DML("DML", "加工层", "63", "数据源类型"),
    SFL("SFL", "系统层", "63", "数据源类型"),
    AML("AML", "AI模型层", "63", "数据源类型"),
    DQC("DQC", "管控层", "63", "数据源类型"),
    UDL("UDL", "自定义层", "63", "数据源类型"),
    KFK("KFK", "流数据层", "63", "数据源类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    DataSourceType(String code, String value, String catCode, String catValue) {
        this.code = code;
        this.value = value;
        this.catCode = catCode;
        this.catValue = catValue;
    }

    public String getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }

    public String getCatCode() {
        return catCode;
    }

    public String getCatValue() {
        return catValue;
    }

    public static final String CodeName = "DataSourceType";

    public static String ofValueByCode(String code) {
        for (DataSourceType typeCode : DataSourceType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DataSourceType:数据源类型]");
    }

    public static DataSourceType ofEnumByCode(String code) {
        for (DataSourceType typeCode : DataSourceType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DataSourceType:数据源类型]");
    }

    public static String getCodeByValue(String value) {
        for (DataSourceType typeCode : DataSourceType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[DataSourceType:数据源类型]");
    }

    public static String ofCatValue() {
        return DataSourceType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return DataSourceType.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemRuntimeException("There‘s no need for you to !");
    }

    public static String Serialized() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(HdfsFileType.class);
            String obj = Base64.getEncoder().encodeToString(baos.toByteArray());
            return obj;
        } catch (Exception e) {
            throw new SystemRuntimeException(e);
        }
    }
}
