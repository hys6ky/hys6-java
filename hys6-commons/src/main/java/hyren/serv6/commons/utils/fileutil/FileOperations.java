package hyren.serv6.commons.utils.fileutil;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.ApplyType;
import hyren.serv6.base.codes.AuthType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.SourceFileAttribute;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.fileutil.FileTypeUtil;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.DboExecute;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class FileOperations {

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "")
    @Return(desc = "", range = "")
    public static byte[] getFileBytesFromAvro(String fileId) {
        Optional<SourceFileAttribute> sourceFileAttribute = Dbo.queryOneObject(SourceFileAttribute.class, "SELECT * FROM source_file_attribute WHERE file_id=?", fileId);
        if (!sourceFileAttribute.isPresent()) {
            throw new BusinessException("申请的文件不存在！fileId=" + fileId);
        }
        String isCache = sourceFileAttribute.get().getIs_cache();
        if (StringUtil.isNotBlank(isCache) && IsFlag.Shi.getValue().equals(isCache)) {
            try {
                String sourcePath = sourceFileAttribute.get().getSource_path();
                return FileUtils.readFileToByteArray(new File(sourcePath));
            } catch (IOException e) {
                e.printStackTrace();
                throw new BusinessException("Failed to get the byte of the local file!" + fileId);
            }
        }
        Long fileAvroBlock = sourceFileAttribute.get().getFile_avro_block();
        String fileAvroPath = sourceFileAttribute.get().getFile_avro_path();
        return ClassBase.avroPInstance().getFileBytesFromAvro(fileAvroBlock, fileAvroPath);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, String> getFileInfoByFileId(String fileId) {
        return getFileInfoByFileId(fileId, false);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "")
    @Param(name = "ocrText", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, String> getFileInfoByFileId(String fileId, boolean ocrText) {
        Map<String, String> fileInfoMap = new HashMap<>();
        SourceFileAttribute source_file_attribute = new SourceFileAttribute();
        source_file_attribute.setFile_id(fileId);
        Optional<SourceFileAttribute> fileRs = Dbo.queryOneObject(SourceFileAttribute.class, "SELECT * FROM source_file_attribute WHERE file_id=?", source_file_attribute.getFile_id());
        if (!fileRs.isPresent()) {
            throw new BusinessException("申请的文件不存在！fileId=" + fileId);
        } else {
            SourceFileAttribute sfa = fileRs.get();
            long fileAvroBlock = sfa.getFile_avro_block();
            String fileAvroPath = sfa.getFile_avro_path();
            String originalName = sfa.getOriginal_name();
            String storageDate = sfa.getStorage_date();
            String storageTime = sfa.getStorage_time();
            String originalUpdateDate = sfa.getOriginal_update_date();
            String originalUpdateTime = sfa.getOriginal_update_time();
            String fileSuffix = sfa.getFile_suffix();
            fileInfoMap.put("original_name", originalName);
            fileInfoMap.put("storage_date", storageDate);
            fileInfoMap.put("storage_time", storageTime);
            fileInfoMap.put("original_update_date", originalUpdateDate);
            fileInfoMap.put("original_update_time", originalUpdateTime);
            Map<String, Object> fileContents = ClassBase.avroPInstance().getFileContents(fileAvroBlock, fileAvroPath);
            String fileText = fileContents.get("fileText").toString();
            List<String> list = FileTypeUtil.getTypeFileList(FileTypeUtil.TuPian);
            assert list != null;
            if (list.contains(fileSuffix)) {
                ByteBuffer bb = (ByteBuffer) fileContents.get("file_contents");
                fileText = Base64.getEncoder().encodeToString(bb.array());
                if (ocrText) {
                    fileText = getOcrText(fileAvroPath, originalName, fileId);
                }
            }
            fileInfoMap.put("file_content", fileText);
            fileInfoMap.put("file_id", fileId);
            fileInfoMap.put("file_suffix", fileSuffix);
        }
        return fileInfoMap;
    }

    private static String getOcrText(String file_avro_path, String name, String uuid) {
        return ClassBase.avroPInstance().getOcrText(file_avro_path, name, uuid);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "")
    @Return(desc = "", range = "")
    private static boolean checkFileIsExist(String fileId) {
        if (StringUtil.isBlank(fileId)) {
            throw new BusinessException("待检查文件id为空！fileId=" + fileId);
        }
        return Dbo.queryNumber("SELECT COUNT(1) FROM " + SourceFileAttribute.TableName + " WHERE file_id=?", fileId).orElseThrow(() -> new BusinessException("检查文件是否存在的SQL编写错误")) == 1;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "")
    public static void updateViewFilePermissions(String fileId) {
        Result dataAuthRs = Dbo.queryResult("select * from data_auth where file_id=? and apply_type = ? and auth_type = ?", fileId, ApplyType.ChaKan.getCode(), AuthType.YiCi.getCode());
        if (!dataAuthRs.isEmpty()) {
            DboExecute.deletesOrThrow(1, "权限更新失败!", "delete from data_auth where file_id=? and apply_type = ? and auth_type = ?", fileId, ApplyType.ChaKan.getCode(), AuthType.YiCi.getCode());
        }
    }
}
