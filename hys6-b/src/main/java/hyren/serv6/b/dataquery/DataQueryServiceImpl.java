package hyren.serv6.b.dataquery;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.CodecUtil;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.fileutil.FileOperations;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Slf4j
@Service
public class DataQueryServiceImpl {

    @Method(desc = "", logicStep = "")
    @Param(name = "original_name", desc = "", range = "", nullable = true)
    @Param(name = "apply_date", desc = "", range = "", nullable = true)
    @Param(name = "apply_type", desc = "", range = "", nullable = true)
    @Param(name = "auth_type", desc = "", range = "", nullable = true)
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> myApplyRecord(String original_name, String apply_date, String apply_type, String auth_type, int currPage, int pageSize) {
        Map<String, Object> myApplyRecordMap = new HashMap<>();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql(" select da.*,sfa.* from source_file_attribute sfa LEFT JOIN (" + " select MAX(concat(apply_date,apply_time)) applytime,file_id,apply_type" + " from data_auth where user_id=?");
        asmSql.addParam(UserUtil.getUserId());
        asmSql.addLikeParam("apply_date", apply_date);
        if (StringUtil.isNotBlank(apply_type)) {
            asmSql.addSql(" and apply_type = ?").addParam(apply_type);
        }
        if (StringUtil.isNotBlank(auth_type)) {
            asmSql.addSql(" and auth_type = ?").addParam(auth_type);
        }
        asmSql.addSql(" GROUP BY file_id,apply_type) a JOIN data_auth da on a.applytime = concat(da.apply_date," + " da.apply_time) ON da.file_id = sfa.file_id where da.apply_type !=''");
        if (StringUtil.isNotBlank(original_name)) {
            asmSql.addLikeParam("original_name", '%' + original_name + '%');
        }
        asmSql.addSql(" ORDER BY da.apply_date DESC,da.apply_time DESC");
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> myApplyRecordRs = Dbo.queryPagedList(page, asmSql.sql(), asmSql.params());
        myApplyRecordMap.put("myApplyRecordRs", myApplyRecordRs);
        myApplyRecordMap.put("totalSize", page.getTotalSize());
        return myApplyRecordMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "da_id", desc = "", range = "")
    public void cancelApply(long da_id) {
        Optional<DataAuth> daRs = Dbo.queryOneObject(DataAuth.class, "select * from " + DataAuth.TableName + " where da_id = ? and auth_type = ?", da_id, AuthType.ShenQing.getCode());
        if (!daRs.isPresent()) {
            throw new BusinessException("取消申请的文件已不存在！da_id=" + da_id);
        }
        DboExecute.deletesOrThrow("取消申请的文件失败!", "DELETE from " + DataAuth.TableName + " where da_id = ?", da_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "apply_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getApplyData(String apply_type) {
        Map<String, Object> applyDataMap = new HashMap<>();
        Object[] sourceIdsObj = Dbo.queryOneColumnList("select source_id from data_source").toArray();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        ApplyType applyType = ApplyType.ofEnumByCode(apply_type);
        if (applyType == ApplyType.ChaKan || applyType == ApplyType.XiaZai || applyType == ApplyType.FaBu || applyType == ApplyType.ChongMingMing) {
            asmSql.addSql("select da.*,sfa.original_name,sfa.file_size,sfa.file_type,sfa.file_suffix from data_auth da" + " left join source_file_attribute sfa on da.file_id = sfa.file_id where da.user_id = ? and" + " da.auth_type=? and da.apply_type = ?");
            asmSql.addParam(UserUtil.getUserId());
            asmSql.addParam(AuthType.ShenQing.getCode());
            asmSql.addParam(apply_type);
            asmSql.addORParam("sfa.source_id", sourceIdsObj);
            List<Map<String, Object>> apply_rs = Dbo.queryList(asmSql.sql(), asmSql.params());
            applyDataMap.put("apply_rs", apply_rs);
            applyDataMap.put("apply_type", apply_type);
        } else {
            throw new BusinessException("不存在的申请类型! apply_type=" + apply_type + ", range={1:查看,2:下载,3,发布,4:重命名}");
        }
        return applyDataMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "", example = "")
    public void viewImage(String fileId) {
        if (!checkFileViewPermissions(fileId)) {
            throw new BusinessException("没有该文件的查看权限,请先申请后再查看!");
        }
        Map<String, String> fileInfoMap = FileOperations.getFileInfoByFileId(fileId);
        if (fileInfoMap.isEmpty()) {
            throw new BusinessException("根据文件id获取文件信息,结果为空!");
        }
        try (OutputStream outputStream = ContextDataHolder.getResponse().getOutputStream()) {
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            byte[] file_contents = Base64.getDecoder().decode(fileInfoMap.get("file_content").getBytes());
            InputStream in = new ByteArrayInputStream(file_contents);
            int len;
            byte[] buf = new byte[1024];
            while ((len = in.read(buf, 0, 1024)) != -1) {
                outputStream.write(buf, 0, len);
            }
            outputStream.flush();
        } catch (IOException ioe) {
            throw new AppSystemException("查看图片失败！！！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    public Map<String, String> viewFile(String fileId) {
        if (!checkFileViewPermissions(fileId)) {
            throw new BusinessException("没有该文件的查看权限,请先申请后再查看!");
        }
        Map<String, String> fileInfoMap = FileOperations.getFileInfoByFileId(fileId);
        if (fileInfoMap.isEmpty()) {
            throw new BusinessException("根据文件id获取文件信息,结果为空!");
        }
        FileOperations.updateViewFilePermissions(fileId);
        return fileInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "", example = "")
    public boolean checkFileViewPermissions(String fileId) {
        Result dataAuthRs = Dbo.queryResult("select * from data_auth where file_id=? and apply_type = ? and auth_type in (?,?)", fileId, ApplyType.ChaKan.getCode(), AuthType.YiCi.getCode(), AuthType.YunXu.getCode());
        return !dataAuthRs.isEmpty();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "", example = "")
    @Param(name = "applyType", desc = "", range = "")
    @Return(desc = "", range = "")
    public void applicationProcessing(String fileId, String applyType) {
        Optional<SourceFileAttribute> fileRs = Dbo.queryOneObject(SourceFileAttribute.class, "SELECT * FROM source_file_attribute WHERE file_id=?", fileId);
        if (!fileRs.isPresent()) {
            throw new BusinessException("申请的文件不存在！fileId=" + fileId);
        }
        Dbo.execute("DELETE FROM DATA_AUTH WHERE file_id = ? AND apply_type = ? AND user_id = ? AND dep_id = ?", fileId, applyType, UserUtil.getUserId(), UserUtil.getUser().getDepId());
        DataAuth dataAuth = new DataAuth();
        dataAuth.setDa_id(PrimayKeyGener.getNextId());
        dataAuth.setApply_date(DateUtil.getSysDate());
        dataAuth.setApply_time(DateUtil.getSysTime());
        dataAuth.setApply_type(applyType);
        dataAuth.setAuth_type(AuthType.ShenQing.getCode());
        dataAuth.setFile_id(fileId);
        dataAuth.setUser_id(UserUtil.getUserId());
        dataAuth.setDep_id(UserUtil.getUser().getDepId());
        dataAuth.setAgent_id(fileRs.get().getAgent_id());
        dataAuth.setSource_id(fileRs.get().getSource_id());
        dataAuth.setCollect_set_id(fileRs.get().getCollect_set_id());
        if ((dataAuth.add(Dbo.db()) != 1)) {
            throw new BusinessException("申请文件失败！fileId=" + fileId);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "", nullable = true)
    @Param(name = "fcsId", desc = "", range = "", nullable = true)
    @Param(name = "fileType", desc = "", range = "", nullable = true)
    @Param(name = "startDate", desc = "", range = "", nullable = true)
    @Param(name = "endDate", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Map<String, Object> getConditionalQuery(String sourceId, String fcsId, String fileType, String startDate, String endDate) {
        Result file_rs = conditionalQuery(sourceId, fcsId, fileType, startDate, endDate);
        Map<String, Object> conditionalQueryMap = new HashMap<>();
        conditionalQueryMap.put("file_rs", file_rs.toList());
        Map<String, Object> fadMap = getFileApplicationDetails();
        int myDownloadRequest = 0;
        int myPostApplication = 0;
        int myApplicationRecord = 0;
        int myRenameRequest = 0;
        int myViewRequest = 0;
        Result applyRequestRs = (Result) fadMap.get("applyRequestRs");
        if (!applyRequestRs.isEmpty()) {
            for (int i = 0; i < applyRequestRs.getRowCount(); i++) {
                ApplyType applyType = ApplyType.ofEnumByCode(applyRequestRs.getString(i, "apply_type"));
                if (ApplyType.XiaZai == applyType) {
                    myDownloadRequest = applyRequestRs.getIntDefaultZero(i, "count");
                } else if (ApplyType.FaBu == applyType) {
                    myPostApplication = applyRequestRs.getIntDefaultZero(i, "count");
                } else if (ApplyType.ChaKan == applyType) {
                    myViewRequest = applyRequestRs.getIntDefaultZero(i, "count");
                } else if (ApplyType.ChongMingMing == applyType) {
                    myRenameRequest = applyRequestRs.getIntDefaultZero(i, "count");
                }
            }
        }
        Result countRs = (Result) fadMap.get("countRs");
        if (!countRs.isEmpty()) {
            myApplicationRecord = countRs.getRowCount();
        }
        conditionalQueryMap.put("myDownloadRequest", myDownloadRequest);
        conditionalQueryMap.put("myPostApplication", myPostApplication);
        conditionalQueryMap.put("myApplicationRecord", myApplicationRecord);
        conditionalQueryMap.put("myViewRequest", myViewRequest);
        conditionalQueryMap.put("myRenameRequest", myRenameRequest);
        return conditionalQueryMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "timesRecently", desc = "", range = "", valueIfNull = "3")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getLast3FileCollections(int timesRecently) {
        List<Map<String, Object>> l3fcList = Dbo.queryList("select storage_date, storage_time," + " max(concat(storage_date,storage_time)) max_date, count(1) count," + " fcs.fcs_name from " + SourceFileAttribute.TableName + " sfa join" + " " + FileCollectSet.TableName + " fcs on sfa.collect_set_id = fcs.fcs_id" + " join agent_info ai on ai.agent_id = fcs.agent_id" + " where collect_type = ? and ai.user_id = ?" + " GROUP BY storage_date,storage_time,fcs.fcs_name" + " ORDER BY max_date desc limit ?", AgentType.WenJianXiTong.getCode(), UserUtil.getUserId(), timesRecently);
        List<Map<String, Object>> last3FileCollectionsMapList = new ArrayList<>();
        for (Map<String, Object> l3fcMap : l3fcList) {
            Map<String, Object> last3FileCollectionsMap = new HashMap<>(30);
            String collectDate = (String) l3fcMap.get("storage_date");
            String collectTime = (String) l3fcMap.get("storage_time");
            Integer collectSum = Integer.valueOf(l3fcMap.get("count").toString());
            String collectName = (String) l3fcMap.get("fcs_name");
            last3FileCollectionsMap.put("collectDate", collectDate);
            last3FileCollectionsMap.put("collectTime", collectTime);
            last3FileCollectionsMap.put("collectName", collectName);
            last3FileCollectionsMap.put("collectSum", collectSum);
            last3FileCollectionsMapList.add(last3FileCollectionsMap);
        }
        return last3FileCollectionsMapList;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getSevenDayCollectFileSum() {
        List<Map<String, Object>> scfList = Dbo.queryList("select count(1) count,storage_date" + " from " + SourceFileAttribute.TableName + " sfa join" + " " + AgentInfo.TableName + " ai on sfa.agent_id = ai.agent_id" + " where sfa.collect_type = ? AND ai.user_id = ? GROUP BY storage_date" + " ORDER BY storage_date desc LIMIT 7", AgentType.WenJianXiTong.getCode(), UserUtil.getUserId());
        List<Map<String, Object>> sevenDayCollectFileSumList = new ArrayList<>();
        if (!scfList.isEmpty()) {
            for (Map<String, Object> scfMap : scfList) {
                Map<String, Object> sevenDayCollectFileSumMap = new HashMap<>();
                String collectDate = (String) scfMap.get("storage_date");
                int collectSum = Integer.parseInt(scfMap.get("count").toString());
                sevenDayCollectFileSumMap.put("collectDate", collectDate);
                sevenDayCollectFileSumMap.put("collectSum", collectSum);
                sevenDayCollectFileSumList.add(sevenDayCollectFileSumMap);
            }
        }
        return sevenDayCollectFileSumList;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getFileClassifySum() {
        List<Map<String, Object>> fcsList = Dbo.queryList("select count(1) sum_num,file_type" + " from " + SourceFileAttribute.TableName + " sfa join " + AgentInfo.TableName + " ai" + " on sfa.agent_id = ai.agent_id where sfa.collect_type = ? AND ai.user_id = ?" + " GROUP BY file_type ORDER BY file_type", AgentType.WenJianXiTong.getCode(), UserUtil.getUserId());
        List<Map<String, Object>> classificationSumList = new ArrayList<>();
        if (!fcsList.isEmpty()) {
            for (Map<String, Object> fcsMap : fcsList) {
                Map<String, Object> classificationSumMap = new HashMap<>();
                classificationSumMap.put(FileType.ofValueByCode((String) fcsMap.get("file_type")), fcsMap.get("sum_num"));
                classificationSumMap.put("file_type", FileType.ofValueByCode((String) fcsMap.get("file_type")));
                classificationSumMap.put("sum_num", fcsMap.get("sum_num"));
                classificationSumList.add(classificationSumMap);
            }
        }
        return classificationSumList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "favId", desc = "", range = "")
    public void cancelFavoriteFile(Long favId) {
        Long userId = UserUtil.getUserId();
        List<UserFav> userFavs = Dbo.queryList(UserFav.class, "select user_id from " + UserFav.TableName + " where fav_id =?", favId);
        if (userFavs == null || userFavs.isEmpty()) {
            throw new BusinessException("不存在该收藏记录，无法取消收藏");
        }
        UserFav userFav = userFavs.get(0);
        if (!userFav.getUser_id().equals(userId)) {
            throw new BusinessException("该文件由 " + userFav.getUser_id() + " 用户收藏！当前用户无法取消收藏！");
        }
        int deleteUserFavNum = Dbo.execute("delete from " + UserFav.TableName + " where fav_id=? and user_id=?", favId, UserUtil.getUserId());
        if (deleteUserFavNum != 1) {
            if (deleteUserFavNum == 0) {
                throw new BusinessException("表中不存在该条记录！favId=" + favId);
            }
            throw new BusinessException("取消收藏失败！favId=" + favId);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "")
    public void saveFavoriteFile(String fileId) {
        Optional<SourceFileAttribute> sourceFileAttribute = Dbo.queryOneObject(SourceFileAttribute.class, "select original_name from " + SourceFileAttribute.TableName + " where" + " file_id = ?", fileId);
        if (!sourceFileAttribute.isPresent()) {
            throw new BusinessException("文件不存在！fileId=" + fileId);
        }
        UserFav userFav = new UserFav();
        userFav.setFav_id(PrimayKeyGener.getNextId());
        userFav.setOriginal_name(sourceFileAttribute.get().getOriginal_name());
        userFav.setFile_id(fileId);
        userFav.setUser_id(UserUtil.getUserId());
        userFav.setFav_flag(IsFlag.Shi.getCode());
        if (userFav.add(Dbo.db()) != 1) {
            throw new BusinessException("收藏失败！fileId=" + userFav.getFile_id());
        }
    }

    public void downloadFile(String fileId, String fileName, String queryKeyword) {
        if (!downloadFileCheck(fileId)) {
            throw new BusinessException("文件没有下载权限! fileName=" + fileName);
        }
        try (OutputStream out = ContextDataHolder.getResponse().getOutputStream()) {
            ContextDataHolder.getResponse().reset();
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.UTF_8.getValue()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            byte[] bye = FileOperations.getFileBytesFromAvro(fileId);
            if (bye == null) {
                throw new BusinessException("文件已不存在! fileName=" + fileName);
            }
            out.write(bye);
            out.flush();
            modifySortCount(fileId, queryKeyword);
        } catch (IOException e) {
            throw new AppSystemException("文件下载失败! fileName=" + fileName + " e: " + e);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getFileCollectionTask(long sourceId) {
        return Dbo.queryList(" select * from " + FileCollectSet.TableName + " fc" + " join " + AgentInfo.TableName + " ai on fc.agent_id = ai.agent_id" + " where ai.source_id = ? AND ai.agent_type = ? and ai.user_id = ?", sourceId, AgentType.WenJianXiTong.getCode(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getFileDataSource() {
        return Dbo.queryList(" select ds.source_id,ds.datasource_name" + " from " + SourceRelationDep.TableName + " srd" + " join " + DataSource.TableName + " ds on srd.source_id = ds.source_id" + " join " + AgentInfo.TableName + " ai on ds.source_id = ai.source_id" + " where srd.dep_id = ? AND ai.agent_type = ? AND ai.user_id = ?" + " GROUP BY ds.source_id,ds.datasource_name", UserUtil.getUser().getDepId(), AgentType.WenJianXiTong.getCode(), UserUtil.getUserId());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "")
    @Return(desc = "", range = "")
    private boolean downloadFileCheck(String fileId) {
        Result authResult = Dbo.queryResult("select * from " + DataAuth.TableName + " da" + " join " + SourceFileAttribute.TableName + " sfa" + " ON sfa.file_id = da.file_id WHERE da.user_id = ?" + " AND sfa.file_id = ? AND da.apply_type = ?", UserUtil.getUserId(), fileId, ApplyType.XiaZai.getCode());
        if (authResult.isEmpty()) {
            throw new BusinessException("没有文件申请下载的信息,请先申请下载! fileId=" + fileId);
        }
        ApplyType applyType = ApplyType.ofEnumByCode(authResult.getString(0, "apply_type"));
        if (ApplyType.XiaZai == applyType) {
            AuthType authType = AuthType.ofEnumByCode(authResult.getString(0, "auth_type"));
            return AuthType.YunXu == authType || AuthType.YiCi == authType;
        }
        return false;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileId", desc = "", range = "")
    @Param(name = "queryKeyword", desc = "", range = "")
    private void modifySortCount(String fileId, String queryKeyword) {
        Result siResult = Dbo.queryResult("select * from " + SearchInfo.TableName + " where file_id = ? and word_name = ?", fileId, queryKeyword);
        SearchInfo searchInfo = new SearchInfo();
        searchInfo.setWord_name(queryKeyword);
        if (siResult.isEmpty()) {
            long nextId = PrimayKeyGener.getNextId();
            searchInfo.setFile_id(fileId);
            searchInfo.setSi_id(nextId);
            if (searchInfo.add(Dbo.db()) != 1) {
                throw new BusinessException("添加文件计数信息失败！data=" + searchInfo);
            }
        } else {
            searchInfo.setFile_id(fileId);
            int execute = Dbo.execute("update search_info set si_count = si_count+1 where" + " file_id = ? and word_name = ?", fileId, queryKeyword);
            if (execute != 1) {
                throw new BusinessException("修改文件计数信息失败！data" + searchInfo);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Param(name = "fcsId", desc = "", range = "")
    @Param(name = "fileType", desc = "", range = "")
    @Param(name = "startDate", desc = "", range = "")
    @Param(name = "endDate", desc = "", range = "")
    @Return(desc = "", range = "")
    private Result conditionalQuery(String sourceId, String fcsId, String fileType, String startDate, String endDate) {
        SourceFileAttribute sourceFileAttribute = new SourceFileAttribute();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select dep_id from " + SourceRelationDep.TableName + " srd left join " + CollectCase.TableName + " cc on srd.source_id = cc.source_id where srd.dep_id = ?").addParam(UserUtil.getUser().getDepId());
        if (StringUtil.isNotBlank(sourceId)) {
            sourceFileAttribute.setSource_id(Long.valueOf(sourceId));
            asmSql.addSql(" AND srd.source_id = ?").addParam(sourceFileAttribute.getSource_id());
        }
        Result queryResult = Dbo.queryResult(asmSql.sql(), asmSql.params());
        Result searchResult = new Result();
        if (!queryResult.isEmpty()) {
            asmSql.clean();
            sourceFileAttribute.setCollect_type(AgentType.WenJianXiTong.getCode());
            asmSql.addSql("select * from " + SourceFileAttribute.TableName + " WHERE collect_type = ?").addParam(sourceFileAttribute.getCollect_type());
            boolean isFirst = true;
            if (StringUtil.isNotBlank(sourceId)) {
                asmSql.addSql(" AND source_id = ?").addParam(sourceFileAttribute.getSource_id());
                if (StringUtil.isNotBlank(fcsId)) {
                    sourceFileAttribute.setCollect_set_id(Long.valueOf(fcsId));
                    asmSql.addSql(" AND collect_set_id = ?").addParam(sourceFileAttribute.getCollect_set_id());
                }
                isFirst = false;
            }
            if (StringUtil.isNotBlank(startDate)) {
                sourceFileAttribute.setStorage_date(startDate);
                asmSql.addSql(" AND storage_date >= ?").addParam(sourceFileAttribute.getStorage_date());
                isFirst = false;
            }
            if (StringUtil.isNotBlank(endDate)) {
                sourceFileAttribute.setStorage_date(endDate);
                asmSql.addSql(" AND storage_date <= ?").addParam(sourceFileAttribute.getStorage_date());
                isFirst = false;
            }
            if (isFirst) {
                asmSql.addSql(" AND storage_date = (SELECT max(storage_date) FROM source_file_attribute sfa JOIN " + "agent_info ai ON sfa.agent_id = ai.agent_id WHERE ai.user_id = ? )").addParam(UserUtil.getUserId());
            }
            asmSql.addSql(" order by file_id");
            searchResult = Dbo.queryResult(asmSql.sql(), asmSql.params());
            if (!searchResult.isEmpty()) {
                for (int i = 0; i < searchResult.getRowCount(); i++) {
                    searchResult.setObject(i, "file_size", FileUtil.fileSizeConversion(searchResult.getLongDefaultZero(i, "file_size")));
                    searchResult.setObject(i, "storage_date", DateUtil.parseStr2DateWith8Char(searchResult.getString(i, "storage_date")));
                    searchResult.setObject(i, "storage_time", DateUtil.parseStr2TimeWith6Char(searchResult.getString(i, "storage_time")));
                    searchResult.setObject(i, "title", searchResult.getString(i, "original_name"));
                    searchResult.setObject(i, "original_name", searchResult.getString(i, "original_name"));
                    searchResult.setObject(i, "is_others_apply", IsFlag.Fou.getCode());
                    searchResult.setObject(i, "auth_type", "");
                    searchResult.setObject(i, "apply_type", "");
                    Result daResult = Dbo.queryResult("select * from data_auth WHERE file_id = ? and user_id = ? and " + " auth_type != ?", searchResult.getString(i, "file_id"), UserUtil.getUserId(), AuthType.BuYunXu.getCode());
                    if (!daResult.isEmpty()) {
                        StringBuilder authType = new StringBuilder();
                        StringBuilder applyType = new StringBuilder();
                        for (int j = 0; j < daResult.getRowCount(); j++) {
                            authType.append(daResult.getString(j, "auth_type")).append(',');
                            applyType.append(daResult.getString(j, "apply_type")).append(',');
                        }
                        authType.delete(authType.length() - 1, authType.length());
                        applyType.delete(applyType.length() - 1, applyType.length());
                        searchResult.setObject(i, "auth_type", authType.toString());
                        searchResult.setObject(i, "apply_type", applyType.toString());
                    }
                }
            }
        } else {
            asmSql.clean();
            asmSql.addSql("select * from source_file_attribute where collect_type = ?").addParam(AgentType.WenJianXiTong.getCode());
            if (!StringUtil.isEmpty(fileType) && FileType.All != FileType.ofEnumByCode(fileType)) {
                asmSql.addSql("AND file_type = ?").addParam(fileType);
            }
            asmSql.addSql(" order by file_id");
            Result sfaRsAll = Dbo.queryResult(asmSql.sql(), asmSql.params());
            if (!sfaRsAll.isEmpty()) {
                String[] authTypes = { AuthType.YunXu.getCode(), AuthType.YiCi.getCode() };
                for (int i = 0; i < sfaRsAll.getRowCount(); i++) {
                    asmSql.clean();
                    asmSql.addSql("select * from data_auth WHERE file_id = ? and  apply_type = ? ").addParam(sfaRsAll.getString(i, "file_id")).addParam(ApplyType.FaBu.getCode()).addORParam("auth_type", authTypes);
                    Result daResult = Dbo.queryResult(asmSql.sql(), asmSql.params());
                    if (!daResult.isEmpty()) {
                        StringBuilder authType = new StringBuilder();
                        StringBuilder applyType = new StringBuilder();
                        for (int j = 0; j < daResult.getRowCount(); j++) {
                            authType.append(daResult.getString(i, "auth_type")).append(',');
                            applyType.append(daResult.getString(i, "apply_type")).append(',');
                            authType.delete(authType.length() - 1, authType.length());
                            applyType.delete(applyType.length() - 1, applyType.length());
                            daResult.setObject(j, "auth_type", authType.toString());
                            daResult.setObject(j, "apply_type", applyType.toString());
                            daResult.setObject(j, "file_id", sfaRsAll.getString(i, "file_id"));
                            daResult.setObject(j, "collect_type", sfaRsAll.getString(i, "collect_type"));
                            daResult.setObject(j, "hbase_name", sfaRsAll.getString(i, "hbase_name"));
                            daResult.setObject(j, "title", sfaRsAll.getString(i, "original_name"));
                            daResult.setObject(j, "original_name", sfaRsAll.getString(i, "original_name"));
                            daResult.setObject(j, "storage_date", DateUtil.parseStr2DateWith8Char(sfaRsAll.getString(i, "storage_date")));
                            daResult.setObject(j, "storage_time", DateUtil.parseStr2TimeWith6Char(sfaRsAll.getString(i, "storage_time")));
                            daResult.setObject(j, "file_size", FileUtil.fileSizeConversion(sfaRsAll.getLongDefaultZero(i, "file_size")));
                            daResult.setObject(j, "file_suffix", sfaRsAll.getString(i, "file_suffix"));
                            daResult.setObject(j, "is_others_apply", IsFlag.Shi.getCode());
                            searchResult.add(daResult);
                        }
                    }
                }
            }
        }
        return searchResult;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getFileApplicationDetails() {
        Map<String, Object> fileApplicationDetails = new HashMap<>();
        Object[] sourceIdsObj = Dbo.queryOneColumnList("select source_id from data_source").toArray();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT apply_type,count(apply_type) count from data_auth da JOIN source_file_attribute sfa" + " ON da.file_id = sfa.file_id where USER_ID = ? and auth_type = ?");
        asmSql.addParam(UserUtil.getUserId());
        asmSql.addParam(AuthType.ShenQing.getCode());
        asmSql.addORParam("sfa.source_id", sourceIdsObj);
        asmSql.addSql(" GROUP BY apply_type");
        Result applyRequestRs = Dbo.queryResult(asmSql.sql(), asmSql.params());
        fileApplicationDetails.put("applyRequestRs", applyRequestRs);
        asmSql.clean();
        asmSql.addSql("select  MAX(apply_date || apply_time) applytime,da.file_id,apply_type from data_auth da JOIN " + "source_file_attribute sfa ON da.file_id = sfa.file_id where user_id=? ");
        asmSql.addParam(UserUtil.getUserId());
        asmSql.addORParam("sfa.source_id", sourceIdsObj);
        asmSql.addSql(" GROUP BY da.file_id,apply_type");
        Result countRs = Dbo.queryResult(asmSql.sql(), asmSql.params());
        fileApplicationDetails.put("countRs", countRs);
        return fileApplicationDetails;
    }
}
