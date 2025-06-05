package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum AutoDataSumType {

    QiuHe("01", "求和", "116", "可视化数据汇总类型"),
    QiuPingJun("02", "求平均", "116", "可视化数据汇总类型"),
    QiuZuiDaZhi("03", "求最大值", "116", "可视化数据汇总类型"),
    QiuZuiXiaoZhi("04", "求最小值", "116", "可视化数据汇总类型"),
    ZongHangShu("05", "总行数", "116", "可视化数据汇总类型"),
    YuanShiShuJu("06", "原始数据", "116", "可视化数据汇总类型"),
    ChaKanQuanBu("07", "查看全部", "116", "可视化数据汇总类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    AutoDataSumType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "AutoDataSumType";

    public static String ofValueByCode(String code) {
        for (AutoDataSumType typeCode : AutoDataSumType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AutoDataSumType:可视化数据汇总类型]");
    }

    public static AutoDataSumType ofEnumByCode(String code) {
        for (AutoDataSumType typeCode : AutoDataSumType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AutoDataSumType:可视化数据汇总类型]");
    }

    public static String getCodeByValue(String value) {
        for (AutoDataSumType typeCode : AutoDataSumType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[AutoDataSumType:可视化数据汇总类型]");
    }

    public static String ofCatValue() {
        return AutoDataSumType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return AutoDataSumType.values()[0].getCatCode();
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
