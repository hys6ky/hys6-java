package hyren.serv6.commons.config.webconfig;

import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.base.exception.internal.FrameworkRuntimeException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import javax.annotation.PostConstruct;
import java.io.File;

@Slf4j
@Getter
@Setter
@ToString
@ConfigurationProperties(ignoreInvalidFields = true)
public class WebinfoProperties {

    @Value("${spring.servlet.multipart.file-size-threshold:10485760}")
    private Integer fileSizeThreshold;

    @Value("${spring.servlet.multipart.max-request-size:104857600}")
    private Integer maxRequestSize;

    @Value("${spring.servlet.multipart.location:/tmp}")
    private String savedDir;

    @Value("${spring.servlet.multipart.max-file-size:10485760}")
    private Integer maxFileSize;

    @Value("${spring.servlet.multipart.enabled:true}")
    private String enabled;

    @Value("${spring.servlet.multipart.resolve-lazily:false}")
    private String resolveLazily;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static int FILE_SIZE_THRESHOLD;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static int MAX_REQUEST_SIZE;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static int MAX_FILE_SIZE;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String ENABLED;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    public static String FileUpload_SavedDirName;

    @PostConstruct
    public void init() {
        FILE_SIZE_THRESHOLD = this.fileSizeThreshold;
        MAX_REQUEST_SIZE = this.maxRequestSize;
        MAX_FILE_SIZE = this.maxFileSize;
        ENABLED = this.enabled;
        File saveDirFile = new File(this.savedDir);
        if (!saveDirFile.exists()) {
            if (!saveDirFile.mkdirs()) {
                log.error(String.format("创建目录%s失败：", this.savedDir));
            }
        }
        FileUpload_SavedDirName = getDirString(this.savedDir);
    }

    private static String getDirString(String path) {
        if (path == null || path.length() == 0)
            return FileUtil.TEMP_DIR_NAME;
        if (StringUtil.isNotBlank(path)) {
            if (path.charAt(path.length() - 1) == FileUtil.PATH_SEPARATOR_CHAR) {
                return path;
            } else {
                return path + FileUtil.PATH_SEPARATOR_CHAR;
            }
        } else {
            return FileUtil.TEMP_DIR_NAME;
        }
    }
}
