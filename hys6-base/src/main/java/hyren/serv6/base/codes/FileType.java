package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum FileType {

    All("1001", "全部文件", "60", "文件类型"),
    TuPian("1002", "图片", "60", "文件类型"),
    WenDang("1003", "文档", "60", "文件类型"),
    PDFFile("1013", "PDF文件", "60", "文件类型"),
    OfficeFile("1023", "office文件", "60", "文件类型"),
    WenBenFile("1033", "文本文件", "60", "文件类型"),
    YaSuoFile("1043", "压缩文件", "60", "文件类型"),
    RiZhiFile("1053", "日志文件", "60", "文件类型"),
    biaoShuJuFile("1063", "表数据文件", "60", "文件类型"),
    ShiPin("1004", "视频", "60", "文件类型"),
    YinPin("1005", "音频", "60", "文件类型"),
    Other("1006", "其它", "60", "文件类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    FileType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "FileType";

    public static String ofValueByCode(String code) {
        for (FileType typeCode : FileType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[FileType:文件类型]");
    }

    public static FileType ofEnumByCode(String code) {
        for (FileType typeCode : FileType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[FileType:文件类型]");
    }

    public static String getCodeByValue(String value) {
        for (FileType typeCode : FileType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[FileType:文件类型]");
    }

    public static String ofCatValue() {
        return FileType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return FileType.values()[0].getCatCode();
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
