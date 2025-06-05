package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum SdmConsumeDestination {

    ShuJuKu("1", "数据库", "129", "流数据管理消费端目的地"),
    Hbase("2", "hbase", "129", "流数据管理消费端目的地"),
    RestFuWu("3", "rest服务", "129", "流数据管理消费端目的地"),
    LiuWenJian("4", "文件", "129", "流数据管理消费端目的地"),
    ErJinZhiWenJian("5", "二进制文件", "129", "流数据管理消费端目的地"),
    Kafka("6", "Kafka", "129", "流数据管理消费端目的地"),
    ZiDingYeWuLei("7", "自定义业务类", "129", "流数据管理消费端目的地");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    SdmConsumeDestination(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "SdmConsumeDestination";

    public static String ofValueByCode(String code) {
        for (SdmConsumeDestination typeCode : SdmConsumeDestination.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmConsumeDestination:流数据管理消费端目的地]");
    }

    public static SdmConsumeDestination ofEnumByCode(String code) {
        for (SdmConsumeDestination typeCode : SdmConsumeDestination.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmConsumeDestination:流数据管理消费端目的地]");
    }

    public static String getCodeByValue(String value) {
        for (SdmConsumeDestination typeCode : SdmConsumeDestination.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[SdmConsumeDestination:流数据管理消费端目的地]");
    }

    public static String ofCatValue() {
        return SdmConsumeDestination.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return SdmConsumeDestination.values()[0].getCatCode();
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
