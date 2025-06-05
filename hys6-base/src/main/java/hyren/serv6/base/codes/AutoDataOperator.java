package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum AutoDataOperator {

    JieYu("01", "介于", "115", "可视化数据操作符"),
    BuJieYu("02", "不介于", "115", "可视化数据操作符"),
    DengYu("03", "等于", "115", "可视化数据操作符"),
    BuDengYu("04", "不等于", "115", "可视化数据操作符"),
    DaYu("05", "大于", "115", "可视化数据操作符"),
    XiaoYu("06", "小于", "115", "可视化数据操作符"),
    DaYuDengYu("07", "大于等于", "115", "可视化数据操作符"),
    XiaoYuDengYu("08", "小于等于", "115", "可视化数据操作符"),
    ZuiDaDeNGe("09", "最大的N个", "115", "可视化数据操作符"),
    ZuiXiaoDeNGe("10", "最小的N个", "115", "可视化数据操作符"),
    WeiKong("11", "为空", "115", "可视化数据操作符"),
    FeiKong("12", "非空", "115", "可视化数据操作符");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    AutoDataOperator(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "AutoDataOperator";

    public static String ofValueByCode(String code) {
        for (AutoDataOperator typeCode : AutoDataOperator.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AutoDataOperator:可视化数据操作符]");
    }

    public static AutoDataOperator ofEnumByCode(String code) {
        for (AutoDataOperator typeCode : AutoDataOperator.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AutoDataOperator:可视化数据操作符]");
    }

    public static String getCodeByValue(String value) {
        for (AutoDataOperator typeCode : AutoDataOperator.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[AutoDataOperator:可视化数据操作符]");
    }

    public static String ofCatValue() {
        return AutoDataOperator.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return AutoDataOperator.values()[0].getCatCode();
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
