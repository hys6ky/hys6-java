package hyren.serv6.b.realtimecollection.util;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.CodecUtil;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Slf4j
@Component
public class KafkaBeanUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "path", desc = "", range = "", nullable = true)
    @Param(name = "paramMap", desc = "", range = "", nullable = true)
    @Param(name = "id", desc = "", range = "")
    public static void writeJsonFile(String path, String paramMap, String fileName) {
        File jsonFile;
        jsonFile = new File(path + File.separator + fileName);
        File parent = jsonFile.getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs()) {
                throw new BusinessException("目录创建失败!" + parent.getAbsolutePath());
            }
        }
        try (BufferedWriter jsonWtriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jsonFile), StandardCharsets.UTF_8), 1024)) {
            jsonWtriter.write(paramMap);
            jsonWtriter.flush();
        } catch (Exception e) {
            throw new BusinessException("写json文件失败");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "rootPath", desc = "", range = "")
    @Param(name = "fileName", desc = "", range = "")
    public static void downloadFile(String rootPath, String fileName) {
        FileInputStream in = null;
        try (OutputStream out = ContextDataHolder.getResponse().getOutputStream()) {
            String filePath = rootPath + File.separator + fileName;
            log.info("=====本地下载文件路径=====" + filePath);
            ContextDataHolder.getResponse().reset();
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.ISO_8859_1.getCode()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            in = new FileInputStream(filePath);
            byte[] bytes = new byte[1024];
            int len;
            while ((len = in.read(bytes)) > 0) {
                out.write(bytes, 0, len);
            }
            out.flush();
        } catch (UnsupportedEncodingException e) {
            throw new BusinessException("不支持的编码异常");
        } catch (FileNotFoundException e) {
            throw new BusinessException("文件不存在，可能目录不存在！");
        } catch (IOException e) {
            throw new BusinessException("下载文件失败！");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error(e.toString());
                }
            }
        }
    }
}
