package hyren.serv6.commons.utils.stream;

import hyren.serv6.base.exception.BusinessException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class FileGZIP {

    private static final Logger logger = LogManager.getLogger();

    public static void gZip(File file, String filePath, String compressFileName) {
        FileOutputStream fos = null;
        TarArchiveOutputStream os = null;
        GZIPOutputStream gzout = null;
        FileOutputStream gzFile = null;
        FileInputStream tarin = null;
        try {
            String pathtar = file.getAbsolutePath() + ".tar";
            File filetar = new File(pathtar);
            fos = new FileOutputStream(filetar);
            os = new TarArchiveOutputStream(fos);
            TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(file);
            tarArchiveEntry.setName(file.getName());
            os.putArchiveEntry(tarArchiveEntry);
            FileInputStream fileInputStream = new FileInputStream(file);
            IOUtils.copy(fileInputStream, os);
            os.closeArchiveEntry();
            IOUtils.closeQuietly(fileInputStream);
            IOUtils.closeQuietly(os);
            if (!file.delete()) {
                throw new BusinessException("删除文件失败!" + file.getAbsolutePath());
            }
            String pathGZ = pathtar + ".gz";
            gzFile = new FileOutputStream(pathGZ);
            gzout = new GZIPOutputStream(gzFile);
            tarin = new FileInputStream(filetar);
            IOUtils.copy(tarin, gzout);
            if (!filetar.delete()) {
                throw new BusinessException("删除文件失败!" + filetar.getAbsolutePath());
            }
            File fileGZ = new File(pathGZ);
            FileUtils.moveToDirectory(fileGZ, new File(filePath + File.separator + compressFileName), true);
        } catch (IOException e) {
            logger.error(e);
        } finally {
            if (tarin != null) {
                try {
                    tarin.close();
                } catch (IOException e) {
                    logger.error(e);
                }
            }
            if (gzout != null) {
                try {
                    gzout.close();
                } catch (IOException e) {
                    logger.error(logger, e);
                }
            }
            if (gzFile != null) {
                try {
                    gzFile.close();
                } catch (IOException e) {
                    logger.error(logger, e);
                }
            }
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    logger.error(logger, e);
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    logger.error(logger, e);
                }
            }
        }
    }
}
