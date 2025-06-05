package hyren.serv6.commons.compress;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.ReduceType;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import java.io.*;

@DocClass(desc = "", author = "zxz", createdate = "2019/10/12 14:29")
@Slf4j
public class DeCompressionUtil {

    private DeCompressionUtil() {
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "filePath", desc = "", range = "")
    @Param(name = "deCompressWay", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean deCompression(String filePath, String deCompressWay) {
        File input = new File(filePath);
        if (!input.exists()) {
            log.info("文件：" + filePath + "不存在！！！");
            return false;
        }
        String dir = FilenameUtils.getFullPathNoEndSeparator(filePath);
        InputStream in = null;
        ArchiveInputStream tin = null;
        try {
            if (StringUtil.isBlank(deCompressWay)) {
                in = new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(input)), true);
            } else if (ReduceType.TAR.getCode().equals(deCompressWay)) {
                in = new TarArchiveInputStream(new BufferedInputStream(new FileInputStream(filePath)));
            } else if (ReduceType.GZ.getCode().equals(deCompressWay)) {
                in = new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(input)), true);
            } else if (ReduceType.ZIP.getCode().equals(deCompressWay)) {
                in = new ZipArchiveInputStream(new BufferedInputStream(new FileInputStream(filePath)));
            } else {
                throw new BusinessException("无法识别压缩格式：" + deCompressWay);
            }
            tin = new TarArchiveInputStream(in);
            ArchiveEntry entry = tin.getNextEntry();
            while (entry != null) {
                File archiveEntryDir = new File(dir, entry.getName());
                if (!new File(archiveEntryDir.getParent()).exists()) {
                    if (!archiveEntryDir.getParentFile().mkdirs()) {
                        throw new BusinessException("创建文件夹失败");
                    }
                }
                if (entry.isDirectory()) {
                    if (!archiveEntryDir.exists()) {
                        if (!archiveEntryDir.mkdir()) {
                            throw new BusinessException("创建文件夹失败");
                        }
                    }
                    entry = tin.getNextEntry();
                    continue;
                }
                OutputStream out = new BufferedOutputStream(new FileOutputStream(archiveEntryDir));
                IOUtils.copy(tin, out);
            }
            return true;
        } catch (Exception e) {
            log.error("解压文件： " + filePath + " 失败！！！", e);
            return false;
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
                if (null != tin) {
                    tin.close();
                }
            } catch (IOException e) {
                log.error("关闭流失败", e);
            }
        }
    }
}
