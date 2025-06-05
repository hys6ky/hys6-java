package hyren.serv6.base.utils.fileutil;

import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import org.springframework.web.bind.annotation.ValueConstants;
import java.io.File;
import java.util.List;

public class FileUploadUtil {

    public static final String FILEINFO_SEPARATOR = ValueConstants.DEFAULT_NONE;

    public static File getUploadedFile(String fileinfo) {
        Validator.notEmpty(fileinfo, "args(fileinfo) must not null!");
        List<String> fileinfoArr = StringUtil.split(fileinfo, FILEINFO_SEPARATOR);
        return new File(fileinfoArr.get(0));
    }

    @Deprecated
    public static String getUploadedFileName(String fileinfo) {
        Validator.notEmpty(fileinfo, "args(fileinfo) must not null!");
        List<String> fileinfoArr = StringUtil.split(fileinfo, FILEINFO_SEPARATOR);
        return fileinfoArr.get(0);
    }

    public static String getOriginalFileName(String fileinfo) {
        Validator.notEmpty(fileinfo, "args(fileinfo) must not null!");
        List<String> fileinfoArr = StringUtil.split(fileinfo, FILEINFO_SEPARATOR);
        return fileinfoArr.get(1);
    }

    public static String getOriginalFileSize(String fileinfo) {
        Validator.notEmpty(fileinfo, "args(fileinfo) must not null!");
        List<String> fileinfoArr = StringUtil.split(fileinfo, FILEINFO_SEPARATOR);
        return fileinfoArr.get(2);
    }

    public static String getOriginalFileType(String fileinfo) {
        Validator.notEmpty(fileinfo, "args(fileinfo) must not null!");
        List<String> fileinfoArr = StringUtil.split(fileinfo, FILEINFO_SEPARATOR);
        return fileinfoArr.get(3);
    }
}
