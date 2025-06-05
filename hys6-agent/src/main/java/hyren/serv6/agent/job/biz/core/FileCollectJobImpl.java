package hyren.serv6.agent.job.biz.core;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.MetaInfoBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.core.filecollectstage.FileCollectUnloadDataStageImpl;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.agent.trans.biz.unstructuredfilecollect.FileCollectJobService;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.FileSource;
import hyren.serv6.commons.utils.MapDBHelper;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.base.utils.fileutil.FileTypeUtil;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/28 15:15")
public class FileCollectJobImpl implements JobInterface {

    private final FileCollectParamBean fileCollectParamBean;

    private final FileSource file_source;

    public FileCollectJobImpl(FileCollectParamBean fileCollectParamBean, FileSource file_source) {
        fileCollectParamBean.setFile_source_id(file_source.getFile_source_id().toString());
        fileCollectParamBean.setFile_source_path(file_source.getFile_source_path());
        this.fileCollectParamBean = fileCollectParamBean;
        this.file_source = file_source;
    }

    @Method(desc = "", logicStep = "")
    @Override
    public JobStatusInfo runJob() {
        String fcs_id = fileCollectParamBean.getFcs_id();
        String file_source_id = fileCollectParamBean.getFile_source_id();
        if (FileCollectJobService.mapQueue.get(file_source_id) == null) {
            ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10);
            FileCollectJobService.mapQueue.put(file_source_id, queue);
        }
        String statusFilePath = Constant.JOBINFOPATH + fcs_id + File.separator + file_source_id + File.separator + Constant.JOBFILENAME;
        JobStatusInfo jobStatus = JobStatusInfoUtil.getStartJobStatusInfo(statusFilePath, file_source_id, "file_collect");
        try (MapDBHelper mapDBHelper = new MapDBHelper(Constant.MAPDBPATH + fcs_id + File.separator + file_source_id, file_source_id + ".db")) {
            ConcurrentMap<String, String> fileNameHTreeMap = mapDBHelper.htMap(file_source_id, 0);
            List<String> newFile = new ArrayList<>();
            List<String> changeFileList = new ArrayList<>();
            String fileSourcePath = file_source.getFile_source_path();
            log.info("file_source_path: " + fileSourcePath);
            List<String> fileTypeList = getFileSuffixList(file_source);
            getNewFileListByFileType(fileSourcePath, fileTypeList, fileNameHTreeMap, newFile, file_source.getIs_other());
            getChangeFileListByFileType(fileSourcePath, fileTypeList, fileNameHTreeMap, changeFileList, file_source.getIs_other());
            JobStageInterface unloadData = new FileCollectUnloadDataStageImpl(fileCollectParamBean, newFile, changeFileList, fileNameHTreeMap, mapDBHelper);
            StageParamInfo stageParamInfo = unloadData.handleStage(new StageParamInfo());
            jobStatus.setStageParamInfo(stageParamInfo);
            FileCollectJobService.mapQueue.remove(file_source_id);
        } catch (Exception e) {
            log.error("文件采集失败" + e.getMessage());
        }
        return jobStatus;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "path", desc = "", range = "")
    @Param(name = "fileTypeList", desc = "", range = "")
    @Param(name = "fileNameHTreeMap", desc = "", range = "")
    @Param(name = "newFile", desc = "", range = "")
    private void getNewFileListByFileType(String path, List<String> fileTypeList, ConcurrentMap<String, String> fileNameHTreeMap, List<String> newFile, String is_other) {
        File[] files;
        if (IsFlag.Shi.getCode().equals(is_other)) {
            files = new File(path).listFiles((file) -> file.isDirectory() || (!fileNameHTreeMap.containsKey(file.getAbsolutePath()) && !fileTypeList.contains(FileNameUtils.getExtension(file.getName()))));
        } else {
            files = new File(path).listFiles((file) -> file.isDirectory() || (!fileNameHTreeMap.containsKey(file.getAbsolutePath()) && fileTypeList.contains(FileNameUtils.getExtension(file.getName()))));
        }
        if (files != null && files.length > 0) {
            for (File file : files) {
                if (file.isDirectory()) {
                    getNewFileListByFileType(file.getAbsolutePath(), fileTypeList, fileNameHTreeMap, newFile, is_other);
                } else {
                    newFile.add(file.getAbsolutePath());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "path", desc = "", range = "")
    @Param(name = "fileTypeList", desc = "", range = "")
    @Param(name = "fileNameHTreeMap", desc = "", range = "")
    @Param(name = "newFile", desc = "", range = "")
    private void getChangeFileListByFileType(String path, List<String> fileTypeList, ConcurrentMap<String, String> fileNameHTreeMap, List<String> changeFileList, String is_other) {
        File[] files;
        if (IsFlag.Shi.getCode().equals(is_other)) {
            if (JobConstant.FILECHANGESTYPEMD5) {
                files = new File(path).listFiles((file) -> {
                    return file.isDirectory() || (fileNameHTreeMap.containsKey(file.getAbsolutePath()) && !fileTypeList.contains(FileNameUtils.getExtension(file.getName())) && (!JsonUtil.toObject(fileNameHTreeMap.get(file.getAbsolutePath()), new TypeReference<Map<String, Object>>() {
                    }).get("file_md5").equals(MD5Util.md5File(file))));
                });
            } else {
                files = new File(path).listFiles((file) -> file.isDirectory() || (fileNameHTreeMap.containsKey(file.getAbsolutePath()) && !fileTypeList.contains(FileNameUtils.getExtension(file.getName())) && (!JsonUtil.toObject(JsonUtil.toJson(fileNameHTreeMap.get(file.getAbsolutePath())), new TypeReference<Map<String, Object>>() {
                }).get("file_md5").equals(String.valueOf(file.lastModified())))));
            }
        } else {
            if (JobConstant.FILECHANGESTYPEMD5) {
                files = new File(path).listFiles((file) -> file.isDirectory() || (fileNameHTreeMap.containsKey(file.getAbsolutePath()) && fileTypeList.contains(FileNameUtils.getExtension(file.getName())) && (!JsonUtil.toObject(JsonUtil.toJson(fileNameHTreeMap.get(file.getAbsolutePath())), new TypeReference<Map<String, Object>>() {
                }).get("file_md5").equals(MD5Util.md5File(file)))));
            } else {
                files = new File(path).listFiles((file) -> file.isDirectory() || (fileNameHTreeMap.containsKey(file.getAbsolutePath()) && fileTypeList.contains(FileNameUtils.getExtension(file.getName())) && (!JsonUtil.toObject(JsonUtil.toJson(fileNameHTreeMap.get(file.getAbsolutePath())), new TypeReference<Map<String, Object>>() {
                }).get("file_md5").equals(String.valueOf(file.lastModified())))));
            }
        }
        if (files != null && files.length > 0) {
            for (File file : files) {
                if (file.isDirectory()) {
                    getChangeFileListByFileType(file.getAbsolutePath(), fileTypeList, fileNameHTreeMap, changeFileList, is_other);
                } else {
                    changeFileList.add(file.getAbsolutePath());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    private static List<String> getFileSuffixList(FileSource file_source) {
        List<String> fileSuffixList = new ArrayList<>();
        Map<String, String[]> fileTypeMap = FileTypeUtil.getFileTypeMap();
        if (IsFlag.Shi.getCode().equals(file_source.getIs_other())) {
            if (IsFlag.Fou.getCode().equals(file_source.getIs_audio())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.YinPin)));
            }
            if (IsFlag.Fou.getCode().equals(file_source.getIs_image())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.TuPian)));
            }
            if (IsFlag.Fou.getCode().equals(file_source.getIs_office())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.OfficeWenJian)));
            }
            if (IsFlag.Fou.getCode().equals(file_source.getIs_pdf())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.PDFWenJian)));
            }
            if (IsFlag.Fou.getCode().equals(file_source.getIs_text())) {
                addTextType(fileSuffixList, fileTypeMap);
            }
            if (IsFlag.Fou.getCode().equals(file_source.getIs_video())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.ShiPin)));
            }
            if (IsFlag.Fou.getCode().equals(file_source.getIs_compress())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.YaSuoWenJian)));
            }
        } else {
            if (IsFlag.Shi.getCode().equals(file_source.getIs_audio())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.YinPin)));
            }
            if (IsFlag.Shi.getCode().equals(file_source.getIs_image())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.TuPian)));
            }
            if (IsFlag.Shi.getCode().equals(file_source.getIs_office())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.OfficeWenJian)));
            }
            if (IsFlag.Shi.getCode().equals(file_source.getIs_pdf())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.PDFWenJian)));
            }
            if (IsFlag.Shi.getCode().equals(file_source.getIs_text())) {
                addTextType(fileSuffixList, fileTypeMap);
            }
            if (IsFlag.Shi.getCode().equals(file_source.getIs_video())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.ShiPin)));
            }
            if (IsFlag.Shi.getCode().equals(file_source.getIs_compress())) {
                fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.YaSuoWenJian)));
            }
            if (!StringUtil.isBlank(file_source.getCustom_suffix())) {
                fileSuffixList.addAll(StringUtil.split(file_source.getCustom_suffix(), "|"));
            }
        }
        return fileSuffixList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileSuffixList", desc = "", range = "")
    @Param(name = "fileTypeMap", desc = "", range = "")
    private static void addTextType(List<String> fileSuffixList, Map<String, String[]> fileTypeMap) {
        fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.WenBenWenJian)));
        fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.RiZhiWenJian)));
        fileSuffixList.addAll(Arrays.asList(fileTypeMap.get(FileTypeUtil.BiaoShuJuWenJian)));
    }

    @Override
    public List<MetaInfoBean> getMetaInfoGroup() {
        return null;
    }

    @Override
    public MetaInfoBean getMetaInfo() {
        return null;
    }

    @Override
    public JobStatusInfo call() {
        return runJob();
    }
}
