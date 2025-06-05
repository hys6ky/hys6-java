package hyren.serv6.base.utils.packutil;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import java.io.*;
import java.util.Base64;
import java.util.zip.*;

@Slf4j
public class ZipUtils {

    private static final int BUFFER = 8192;

    public static void compress(String jarPath, String... pathName) {
        ZipOutputStream out = null;
        File zipFile = new File(jarPath);
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(zipFile);
            CheckedOutputStream cos = new CheckedOutputStream(fileOutputStream, new Adler32());
            out = new ZipOutputStream(cos);
            String basedir = "";
            for (int i = 0; i < pathName.length; i++) {
                compress(new File(pathName[i]), out, basedir);
            }
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        } finally {
            try {
                if (null != out) {
                    out.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void compress(File file, ZipOutputStream out, String basedir) {
        if (file.isDirectory()) {
            compressDirectory(file, out, basedir);
        } else {
            compressFile(file, out, basedir);
        }
    }

    private static void compressDirectory(File dir, ZipOutputStream out, String basedir) {
        if (!dir.exists())
            return;
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            compress(files[i], out, basedir + dir.getName() + "/");
        }
    }

    private static void compressFile(File file, ZipOutputStream out, String basedir) {
        if (!file.exists()) {
            return;
        }
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(new FileInputStream(file));
            ZipEntry entry = new ZipEntry(basedir + file.getName());
            out.putNextEntry(entry);
            int count;
            byte[] data = new byte[BUFFER];
            while ((count = bis.read(data, 0, BUFFER)) != -1) {
                out.write(data, 0, count);
            }
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        } finally {
            if (null != bis) {
                try {
                    bis.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public static String gzip(String primStr) {
        if (StringUtil.isEmpty(primStr)) {
            return primStr;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(primStr.getBytes());
        } catch (IOException e) {
            log.debug(e.getMessage());
            return null;
        }
        return Base64.getEncoder().encodeToString(out.toByteArray());
    }

    public static String gunzip(String compressedStr) {
        if (StringUtil.isEmpty(compressedStr)) {
            return compressedStr;
        }
        byte[] compressed = Base64.getDecoder().decode(compressedStr);
        String decompressed;
        try (ByteArrayInputStream in = new ByteArrayInputStream(compressed);
            GZIPInputStream ginzip = new GZIPInputStream(in);
            ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int offset;
            while ((offset = ginzip.read(buffer)) != -1) {
                out.write(buffer, 0, offset);
            }
            decompressed = out.toString();
        } catch (IOException e) {
            log.debug(e.getMessage());
            return null;
        }
        return decompressed;
    }

    public static String zip(String str) {
        if (StringUtil.isEmpty(str)) {
            return str;
        }
        byte[] compressed;
        String compressedStr;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
            ZipOutputStream zout = new ZipOutputStream(out)) {
            zout.putNextEntry(new ZipEntry("0"));
            zout.write(str.getBytes());
            zout.closeEntry();
            compressed = out.toByteArray();
            compressedStr = Base64.getEncoder().encodeToString(compressed);
        } catch (IOException e) {
            log.debug(e.getMessage());
            return null;
        }
        return compressedStr;
    }

    public static String unzip(String compressedStr) {
        if (StringUtil.isEmpty(compressedStr)) {
            return compressedStr;
        }
        String decompressed;
        byte[] compressed = Base64.getDecoder().decode(compressedStr);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayInputStream in = new ByteArrayInputStream(compressed);
            ZipInputStream zin = new ZipInputStream(in)) {
            zin.getNextEntry();
            byte[] buffer = new byte[1024];
            int offset = -1;
            while ((offset = zin.read(buffer)) != -1) {
                out.write(buffer, 0, offset);
            }
            decompressed = out.toString();
        } catch (IOException e) {
            log.debug(e.getMessage());
            return null;
        }
        return decompressed;
    }
}
