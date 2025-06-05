package hyren.serv6.base.utils.fileutil;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.codes.FileType;
import java.util.*;

@DocClass(desc = "", author = "zxz", createdate = "2019/10/29 14:23")
public class FileTypeUtil {

    public static String TuPian = "图片";

    public static String PDFWenJian = "PDF文件";

    public static String OfficeWenJian = "office文件";

    public static String WenBenWenJian = "文本文件";

    public static String RiZhiWenJian = "日志文件";

    public static String ShiPin = "视频";

    public static String YinPin = "音频";

    public static String YaSuoWenJian = "压缩文件";

    public static String BiaoShuJuWenJian = "表数据文件";

    private static final Map<String, String[]> fileTypeMap = new HashMap<>();

    private static final Map<String, String[]> fileTypeCode = new HashMap<>();

    private static final List<String> allFileType = new ArrayList<>();

    static {
        String[] picture = { "bmp", "jpg", "tiff", "gif", "pcx", "tga", "exif", "fpx", "svg", "psd", "cdr", "pcd", "dxf", "ufo", "eps", "ai", "png", "raw", "jpeg", "" };
        fileTypeMap.put(TuPian, picture);
        fileTypeCode.put(FileType.TuPian.getCode(), picture);
        String[] pdf = { "pdf" };
        fileTypeMap.put(PDFWenJian, pdf);
        fileTypeCode.put(FileType.PDFFile.getCode(), pdf);
        String[] Office = { "doc", "docx", "xlsx", "xls", "pptx", "ppt" };
        fileTypeMap.put(OfficeWenJian, Office);
        fileTypeCode.put(FileType.OfficeFile.getCode(), Office);
        String[] txt = { "txt", "bat", "c", "bas", "prg", "cmd", "java", "jsp", "xml" };
        fileTypeMap.put(WenBenWenJian, txt);
        fileTypeCode.put(FileType.WenBenFile.getCode(), txt);
        String[] log = { "log" };
        fileTypeMap.put(RiZhiWenJian, log);
        fileTypeCode.put(FileType.RiZhiFile.getCode(), log);
        String[] video = { "avi", "rmvb", "rm", "asf", "divx", "mpg", "mpeg", "mpe", "wmv", "mp4", "mkv", "vob" };
        fileTypeMap.put(ShiPin, video);
        fileTypeCode.put(FileType.ShiPin.getCode(), video);
        String[] audio = { "cd", "ogg", "mp3", "asf", "wma", "mp3pro", "real", "ape", "module", "midi", "vqf" };
        fileTypeMap.put(YinPin, audio);
        fileTypeCode.put(FileType.YinPin.getCode(), audio);
        String[] compression = { "zip", "rar", "jar", "7z", "gz", "arj", "gzip", "tar", "iso" };
        fileTypeMap.put(YaSuoWenJian, compression);
        fileTypeCode.put(FileType.YaSuoFile.getCode(), compression);
        String[] csv = { "csv" };
        fileTypeMap.put(BiaoShuJuWenJian, csv);
        fileTypeCode.put(FileType.biaoShuJuFile.getCode(), csv);
        allFileType.add(TuPian);
        allFileType.add(PDFWenJian);
        allFileType.add(OfficeWenJian);
        allFileType.add(WenBenWenJian);
        allFileType.add(RiZhiWenJian);
        allFileType.add(ShiPin);
        allFileType.add(YinPin);
        allFileType.add(YaSuoWenJian);
        allFileType.add(BiaoShuJuWenJian);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "type", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String fileTypeCode(String type) {
        String fileType = FileType.Other.getCode();
        for (Map.Entry<String, String[]> suffix : fileTypeCode.entrySet()) {
            if (Arrays.asList(suffix.getValue()).contains(type.toLowerCase())) {
                fileType = suffix.getKey();
            }
        }
        return fileType;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "type", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<String> getTypeFileList(String type) {
        String[] types = fileTypeMap.get(type);
        if (types != null) {
            return Arrays.asList(types);
        } else {
            return null;
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<String> getAllFileSuffixList() {
        List<String> fileSuffixList = new ArrayList<>();
        for (String key : getFileTypeMap().keySet()) {
            fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(key)));
        }
        return fileSuffixList;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static List<String> getAllFileType() {
        return allFileType;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, String[]> getFileTypeMap() {
        return fileTypeMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, String[]> getFileTypeCode() {
        return fileTypeCode;
    }
}
