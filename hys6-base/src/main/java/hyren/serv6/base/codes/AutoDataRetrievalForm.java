package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum AutoDataRetrievalForm {

    WenBenKuang("01", "文本框", "106", "自主取数数据展现形式"),
    XiaLaXuanZe("02", "下拉选择", "106", "自主取数数据展现形式"),
    XiaLaDuoXuan("03", "下拉多选", "106", "自主取数数据展现形式"),
    DanXuanAnNiu("04", "单选按钮", "106", "自主取数数据展现形式"),
    FuXuanAnNiu("05", "复选按钮", "106", "自主取数数据展现形式"),
    RiQiXuanZe("06", "日期选择", "106", "自主取数数据展现形式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    AutoDataRetrievalForm(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "AutoDataRetrievalForm";

    public static String ofValueByCode(String code) {
        for (AutoDataRetrievalForm typeCode : AutoDataRetrievalForm.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AutoDataRetrievalForm:自主取数数据展现形式]");
    }

    public static AutoDataRetrievalForm ofEnumByCode(String code) {
        for (AutoDataRetrievalForm typeCode : AutoDataRetrievalForm.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AutoDataRetrievalForm:自主取数数据展现形式]");
    }

    public static String getCodeByValue(String value) {
        for (AutoDataRetrievalForm typeCode : AutoDataRetrievalForm.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[AutoDataRetrievalForm:自主取数数据展现形式]");
    }

    public static String ofCatValue() {
        return AutoDataRetrievalForm.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return AutoDataRetrievalForm.values()[0].getCatCode();
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
