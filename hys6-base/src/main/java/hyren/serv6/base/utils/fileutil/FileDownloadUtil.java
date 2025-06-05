package hyren.serv6.base.utils.fileutil;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.CodecUtil;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import lombok.extern.slf4j.Slf4j;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Base64;

@DocClass(desc = "", author = "dhw", createdate = "2019/12/19 16:50")
@Slf4j
public class FileDownloadUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "directory", desc = "", range = "")
    @Param(name = "sshDetails", desc = "", range = "")
    public static void deleteLogFileBySFTP(String directory, SSHDetails sshDetails) {
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            sshOperate.channelSftp.rm(directory);
            log.info("###########删除文件成功===");
            sshOperate.channelSftp.quit();
        } catch (Exception e) {
            throw new AppSystemException(e);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "remotePath", desc = "", range = "")
    @Param(name = "localPath", desc = "", range = "")
    @Param(name = "sshDetails", desc = "", range = "")
    public static void downloadLogFile(String remotePath, String localPath, SSHDetails sshDetails) {
        OutputStream outputStream = null;
        try (SSHOperate sshOperate = new SSHOperate(sshDetails)) {
            log.info("==========文件下载远程路径remotePath=========" + remotePath);
            log.info("==========文件下载本地路径localPath=========" + localPath);
            File localFile = new File(localPath);
            outputStream = new FileOutputStream(localFile);
            sshOperate.channelSftp.get(remotePath, outputStream);
            log.info("###########下载文件成功===");
            sshOperate.channelSftp.quit();
        } catch (FileNotFoundException e) {
            throw new BusinessException("找不到文件" + e);
        } catch (Exception e) {
            log.info("文件下载失败原因：" + e);
            throw new AppSystemException(e);
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "filePath", desc = "", range = "")
    public static void downloadFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new BusinessException("文件不存在：" + file.getAbsolutePath());
        }
        OutputStream out = null;
        InputStream in = null;
        try {
            HttpServletResponse response = ContextDataHolder.getResponse();
            response.reset();
            response.setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(file.getName().getBytes(CodecUtil.UTF8_CHARSET)));
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(file.getName().getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.UTF_8.getValue()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(file.getName().getBytes(CodecUtil.UTF8_CHARSET)));
            }
            response.setContentType("APPLICATION/OCTET-STREAM");
            in = new FileInputStream(filePath);
            out = response.getOutputStream();
            byte[] bytes = new byte[1024];
            int len;
            while ((len = in.read(bytes)) > 0) {
                out.write(bytes, 0, len);
            }
            out.flush();
        } catch (UnsupportedEncodingException e) {
            throw new BusinessException("不支持的编码异常" + e);
        } catch (FileNotFoundException e) {
            throw new BusinessException("文件不存在，可能目录不存在！" + e);
        } catch (IOException e) {
            throw new BusinessException("下载文件失败！" + e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
}
