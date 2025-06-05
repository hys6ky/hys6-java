package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum ETLDataSource {

    DBWenJianCaiJi("01", "DB文件采集", "150", "ETL作业数据来源"),
    ShuJuKuCaiJi("02", "数据库采集", "150", "ETL作业数据来源"),
    ShuJuKuChouShu("03", "数据库抽数", "150", "ETL作业数据来源"),
    BanJieGouHuaCaiJi("04", "半结构化采集", "150", "ETL作业数据来源"),
    FeiJieGouHuaCaiJi("05", "非结构化采集", "150", "ETL作业数据来源"),
    ShuJuJiaGong("06", "数据加工", "150", "ETL作业数据来源"),
    ShuJuGuanKong("07", "数据管控", "150", "ETL作业数据来源"),
    ShuJuFenFa("08", "数据分发", "150", "ETL作业数据来源"),
    Other("09", "其他", "150", "ETL作业数据来源"),
    ShuJuZhiBiao("10", "数据指标", "150", "ETL作业数据来源"),
    ShuJuLuoBiao("11", "数据落标", "150", "ETL作业数据来源"),
    YuanShuJuCaiJi("12", "元数据采集", "150", "ETL作业数据来源"),
    YuanShuJuDuiBiao("13", "元数据对标", "150", "ETL作业数据来源");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    ETLDataSource(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "ETLDataSource";

    public static String ofValueByCode(String code) {
        for (ETLDataSource typeCode : ETLDataSource.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[ETLDataSource:ETL作业数据来源]");
    }

    public static ETLDataSource ofEnumByCode(String code) {
        for (ETLDataSource typeCode : ETLDataSource.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[ETLDataSource:ETL作业数据来源]");
    }

    public static String getCodeByValue(String value) {
        for (ETLDataSource typeCode : ETLDataSource.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[ETLDataSource:ETL作业数据来源]");
    }

    public static String ofCatValue() {
        return ETLDataSource.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return ETLDataSource.values()[0].getCatCode();
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
