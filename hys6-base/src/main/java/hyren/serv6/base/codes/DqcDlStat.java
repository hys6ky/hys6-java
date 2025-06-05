package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum DqcDlStat {

    DengDaiChuLi("w", "等待处理", "79", "数据质量处理状态"),
    YiTuiHui("b", "已退回", "79", "数据质量处理状态"),
    YiHuLue("i", "已忽略", "79", "数据质量处理状态"),
    YiChuLi("d", "已处理", "79", "数据质量处理状态"),
    ChuLiWanJie("oki", "处理完结", "79", "数据质量处理状态"),
    YiHuLueTongGuo("okd", "已忽略通过", "79", "数据质量处理状态"),
    ZhengChang("zc", "正常", "79", "数据质量处理状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    DqcDlStat(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "DqcDlStat";

    public static String ofValueByCode(String code) {
        for (DqcDlStat typeCode : DqcDlStat.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DqcDlStat:数据质量处理状态]");
    }

    public static DqcDlStat ofEnumByCode(String code) {
        for (DqcDlStat typeCode : DqcDlStat.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DqcDlStat:数据质量处理状态]");
    }

    public static String getCodeByValue(String value) {
        for (DqcDlStat typeCode : DqcDlStat.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[DqcDlStat:数据质量处理状态]");
    }

    public static String ofCatValue() {
        return DqcDlStat.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return DqcDlStat.values()[0].getCatCode();
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
