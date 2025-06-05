package hyren.serv6.b.datasource;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.bean.SourceDepInfo;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.AuthType;
import hyren.serv6.base.codes.MenuType;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.utils.DboExecute;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Slf4j
@Service
public class DataSourceServiceImpl {

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    public void downloadFile(long source_id) {
        HttpServletResponse response = ContextDataHolder.getResponse();
        try (OutputStream outputStream = response.getOutputStream()) {
            Map<String, Object> collectionMap = new HashMap<>();
            addDataSourceToMap(source_id, collectionMap);
            List<SourceRelationDep> sourceRelationDepList = addSourceRelationDepToMap(source_id, collectionMap);
            addDepartmentInfoToMap(collectionMap, sourceRelationDepList);
            List<AgentInfo> agentInfoList = getAgentInfoList(source_id, collectionMap);
            addAgentDownInfoToMap(collectionMap, agentInfoList);
            addCollectJobClassifyToMap(collectionMap, agentInfoList);
            Result ftpCollectResult = getFtpCollectResult(collectionMap, agentInfoList);
            addFtpTransferedToMap(collectionMap, ftpCollectResult);
            Result objectCollectResult = getObjectCollectResult(collectionMap, agentInfoList);
            Result objectCollectTaskResult = getObjectCollectTaskResult(collectionMap, objectCollectResult);
            addObjectCollectStructResultToMap(collectionMap, objectCollectTaskResult);
            Result databaseSetResult = getDatabaseSetResult(collectionMap, agentInfoList);
            Result fileCollectSetResult = getFileCollectSetResult(collectionMap, agentInfoList);
            addFileSourceToMap(collectionMap, fileCollectSetResult);
            addSignalFileToMap(collectionMap, databaseSetResult);
            Result tableInfoResult = getTableInfoResult(collectionMap, databaseSetResult);
            addColumnMergeToMap(collectionMap, tableInfoResult);
            addTableStorageInfoToMap(collectionMap, tableInfoResult);
            addTableCleanToMap(collectionMap, tableInfoResult);
            Result tableColumnResult = getTableColumnResult(collectionMap, tableInfoResult);
            addColumnCleanToMap(collectionMap, tableColumnResult);
            addColumnSplitToMap(collectionMap, tableColumnResult);
            getTableCycle(collectionMap, tableInfoResult);
            byte[] bytes = Base64.getEncoder().encode(JsonUtil.toJson(collectionMap).getBytes(CodecUtil.UTF8_CHARSET));
            if (bytes == null) {
                throw new BusinessException("此文件不存在");
            }
            response.reset();
            response.setCharacterEncoding(CodecUtil.UTF8_STRING);
            response.setContentType("APPLICATION/OCTET-STREAM");
            outputStream.write(bytes);
            outputStream.flush();
        } catch (IOException e) {
            throw new BusinessException("下载文件失败");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "da_id", desc = "", range = "")
    @Param(name = "auth_type", desc = "", range = "")
    public void dataAudit(long da_id, String auth_type) {
        AuthType.ofEnumByCode(auth_type);
        if (Dbo.queryNumber("select count(*) from " + DataAuth.TableName + " where da_id=?", da_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException("此申请已取消或不存在！");
        }
        DataAuth dataAuth = new DataAuth();
        dataAuth.setAudit_date(DateUtil.getSysDate());
        dataAuth.setAudit_time(DateUtil.getSysTime());
        dataAuth.setAudit_userid(getUserId());
        dataAuth.setAudit_name(UserUtil.getUser().getUsername());
        dataAuth.setAuth_type(auth_type);
        dataAuth.setDa_id(da_id);
        dataAuth.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    public void saveDataSource(SourceDepInfo sourceDepInfo) {
        DataSource dataSource = sourceDepInfo.getDataSource();
        Long[] depIds = sourceDepInfo.getDepIds();
        fieldLegalityValidation(dataSource.getDatasource_name(), dataSource.getDatasource_number(), depIds);
        isExistDataSourceNumber(dataSource.getDatasource_number());
        isExistDataSourceName(dataSource.getDatasource_name());
        dataSource.setSource_id(PrimayKeyGener.getNextId());
        dataSource.setCreate_user_id(getUserId());
        dataSource.setCreate_date(DateUtil.getSysDate());
        dataSource.setCreate_time(DateUtil.getSysTime());
        dataSource.add(Dbo.db());
        saveSourceRelationDep(dataSource.getSource_id(), depIds);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDepInfo", desc = "", range = "")
    public void updateDataSource(SourceDepInfo sourceDepInfo) {
        DataSource dataSource = sourceDepInfo.getDataSource();
        Validator.notNull(dataSource);
        Long source_id = dataSource.getSource_id();
        Validator.notNull(source_id);
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " where source_id=? and create_user_id=?", source_id, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) == 0) {
            throw new BusinessException("数据权限校验失败，该用户对应数据源不存在！");
        }
        DataSource data_source = Dbo.queryOneObject(DataSource.class, "select * from " + DataSource.TableName + " where source_id=?", source_id).orElseThrow(() -> new BusinessException("sql查询错误或者数据映射实体错误"));
        String datasource_number = dataSource.getDatasource_number();
        String datasource_name = dataSource.getDatasource_name();
        if (!datasource_number.equals(data_source.getDatasource_number())) {
            throw new BusinessException("编辑时数据源编号不能被修改");
        }
        Long[] depIds = sourceDepInfo.getDepIds();
        fieldLegalityValidation(datasource_name, datasource_number, depIds);
        String source_remark = dataSource.getSource_remark();
        if (data_source.getDatasource_name().equals(datasource_name)) {
            Dbo.execute("update " + DataSource.TableName + " set source_remark=? where source_id=?", source_remark, source_id);
        } else {
            isExistDataSourceName(datasource_name);
            Dbo.execute("update " + DataSource.TableName + " set source_remark=?,datasource_name=? " + " where source_id=?", source_remark, datasource_name, source_id);
        }
        int num = Dbo.execute("delete from " + SourceRelationDep.TableName + " where source_id=?", source_id);
        if (num < 1) {
            throw new BusinessException("编辑时会先删除原数据源与部门关系信息，删除错旧关系时错误");
        }
        saveSourceRelationDep(source_id, depIds);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "source_id", desc = "", range = "")
    public void deleteDataSource(long source_id) {
        if (Dbo.queryNumber("SELECT count(1) FROM " + AgentInfo.TableName + " WHERE source_id=? and user_id=?", source_id, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("此数据源下还有agent，不能删除,sourceId=" + source_id);
        }
        DboExecute.deletesOrThrow("删除数据源信息表失败，sourceId=" + source_id, "delete from " + DataSource.TableName + " where source_id=? and create_user_id=?", source_id, getUserId());
        int srdNum = Dbo.execute("delete from " + SourceRelationDep.TableName + " where source_id=?", source_id);
        if (srdNum < 1) {
            throw new BusinessException("删除该数据源下数据源与部门关系表数据错误:" + source_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "file", desc = "", range = "")
    public void uploadFile(String agent_ip, String agent_port, Long user_id, MultipartFile file) {
        File uploadedFile = null;
        try {
            checkAgentField(agent_ip, agent_port);
            String originalFilename = file.getOriginalFilename();
            if (null == originalFilename) {
                throw new BusinessException("上传文件不存在！");
            }
            uploadedFile = new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + originalFilename);
            file.transferTo(uploadedFile);
            if (!uploadedFile.exists()) {
                throw new BusinessException("上传文件不存在！");
            }
            String strTemp = new String(Base64.getDecoder().decode(Files.readAllBytes(uploadedFile.toPath())));
            Long userId = getUserId();
            if (null == userId) {
                throw new BusinessException("获取当前登录用户信息失败");
            }
            importDataSource(strTemp, agent_ip, agent_port, user_id, userId);
        } catch (IOException e) {
            throw new BusinessException("上传文件失败！");
        } finally {
            if (null != uploadedFile && uploadedFile.exists()) {
                if (!uploadedFile.delete()) {
                    log.error("删除上传文件失败");
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<SysUser> searchDataCollectUser() {
        return Dbo.queryList(SysUser.class, "SELECT distinct user_id,user_name,user_email,user_remark FROM " + SysUser.TableName + " t1" + " JOIN " + SysRole.TableName + " t2 on t1.role_id = t2.role_id" + " JOIN " + RoleMenu.TableName + " t3 ON t2.role_id = t3.role_id " + " JOIN " + ComponentMenu.TableName + " t4 ON t3.menu_id = t4.menu_id where menu_type = ?", MenuType.CaoZhuoYuan.getCode());
    }

    public Map<String, Object> searchDataSourceById(long source_id) {
        Map<String, Object> datasourceMap = Dbo.queryOneObject("select * from " + DataSource.TableName + " where source_id=? and create_user_id=?", source_id, getUserId());
        Result depNameAndId = getDepNameAndId(source_id);
        datasourceMap.put("depNameAndId", depNameAndId.toList());
        return datasourceMap;
    }

    public void updateAuditSourceRelationDep(Long source_id, Long[] dep_id) {
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " ds left join " + SourceRelationDep.TableName + " srd on ds.source_id=srd.source_id where ds.source_id=?" + " and ds.create_user_id=?", source_id, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) == 0) {
            throw new BusinessException("数据权限校验失败，数据不可访问！");
        }
        int num = Dbo.execute("delete from " + SourceRelationDep.TableName + " where source_id=?", source_id);
        if (num < 1) {
            throw new BusinessException("编辑时会先删除原数据源与部门关系信息，删除错旧关系时错误，" + "sourceId=" + source_id);
        }
        saveSourceRelationDep(source_id, dep_id);
    }

    public Result searchSourceRelationDepForPage(int currPage, int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        Result dsResult = Dbo.queryPagedResult(page, "SELECT source_id,datasource_name " + " from " + DataSource.TableName + " where create_user_id=? " + " order by create_date desc,create_time desc", getUserId());
        if (!dsResult.isEmpty()) {
            for (int i = 0; i < dsResult.getRowCount(); i++) {
                Result depNameAndId = getDepNameAndId(dsResult.getLong(i, "source_id"));
                if (!depNameAndId.isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    for (int j = 0; j < depNameAndId.getRowCount(); j++) {
                        sb.append(depNameAndId.getString(j, "dep_name")).append(",");
                    }
                    dsResult.setObject(i, "dep_name", sb.deleteCharAt(sb.length() - 1).toString());
                }
            }
        }
        dsResult.setObject(0, "totalSize", page.getTotalSize());
        return dsResult;
    }

    public Result getDataAuditInfoForPage(int currPage, int pageSize) {
        List<Long> sourceIdList = Dbo.queryOneColumnList("select source_id from " + DataSource.TableName + " where create_user_id=?", getUserId());
        List<Long> userIds = Dbo.queryOneColumnList("select user_id from " + SysUser.TableName + " su" + " JOIN " + SysRole.TableName + " sr on su.role_id = sr.role_id where sr.is_admin='00'");
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        userIds.add(getUserId());
        asmSql.addSql("select da.DA_ID,da.APPLY_DATE,da.APPLY_TIME,da.APPLY_TYPE,da.AUTH_TYPE," + "sfa.original_name, sfa.file_suffix,sfa.file_type,su.user_name FROM " + DataAuth.TableName + " da join " + SysUser.TableName + " su on da.user_id=su.user_id join " + SourceFileAttribute.TableName + " sfa on da.file_id= sfa.file_id");
        if (!userIds.isEmpty()) {
            asmSql.addORParam("su.create_id", userIds.toArray());
        }
        if (!sourceIdList.isEmpty()) {
            asmSql.addORParam("sfa.source_id", sourceIdList.toArray());
        }
        asmSql.addSql(" ORDER BY da_id desc");
        Page page = new DefaultPageImpl(currPage, pageSize);
        Result result = Dbo.queryPagedResult(page, asmSql.sql(), asmSql.params());
        result.setObject(0, "totalSize", page.getTotalSize());
        return result;
    }

    public Result searchDataSourceAndAgentCount() {
        Result dsResult = Dbo.queryResult("SELECT source_id,datasource_name FROM " + DataSource.TableName + " WHERE create_user_id=?", getUserId());
        if (!dsResult.isEmpty()) {
            for (int i = 0; i < dsResult.getRowCount(); i++) {
                long number = Dbo.queryNumber("select count(*) from " + AgentInfo.TableName + " where source_id=?", dsResult.getLong(i, "source_id")).orElseThrow(() -> new BusinessException("sql查询错误！"));
                dsResult.setObject(i, "sumAgent", number);
            }
        }
        return dsResult;
    }

    public List<Map<String, Object>> getTreeDataSourceAndAgentInfo() {
        List<DataSource> dataSourceList = Dbo.queryList(DataSource.class, "SELECT * FROM " + DataSource.TableName + " WHERE create_user_id = ?", getUserId());
        List<AgentInfo> agentInfoList = Dbo.queryList(AgentInfo.class, "SELECT * FROM " + AgentInfo.TableName);
        Map<Long, List<AgentInfo>> agentInfoMap = new HashMap<>();
        if (!agentInfoList.isEmpty()) {
            agentInfoMap = agentInfoList.stream().collect(Collectors.groupingBy(AgentInfo::getSource_id));
        }
        List<Map<String, Object>> treeList = new ArrayList<>();
        for (DataSource dataSource : dataSourceList) {
            Map<String, Object> oneTreeMap = new HashMap<>();
            Long source_id = dataSource.getSource_id();
            oneTreeMap.put("source_id", source_id);
            oneTreeMap.put("label", dataSource.getDatasource_number() + " [ " + dataSource.getDatasource_name() + " ]");
            oneTreeMap.put("parent_id", "0");
            oneTreeMap.put("description", dataSource.getSource_remark());
            if (!agentInfoMap.isEmpty()) {
                List<AgentInfo> agentInfos = agentInfoMap.get(source_id);
                if (!agentInfos.isEmpty()) {
                    List<Map<String, Object>> twoTreeList = new ArrayList<>();
                    for (AgentInfo agentInfo : agentInfos) {
                        Map<String, Object> twoTreeMap = new HashMap<>();
                        twoTreeMap.put("agent_id", agentInfo.getAgent_id());
                        twoTreeMap.put("label", agentInfo.getAgent_name());
                        twoTreeMap.put("parent_id", source_id);
                        String agent_type = agentInfo.getAgent_type();
                        twoTreeMap.put("agent_type", agent_type);
                        twoTreeMap.put("description", AgentType.ofValueByCode(agent_type));
                        twoTreeList.add(twoTreeMap);
                    }
                    oneTreeMap.put("children", twoTreeList);
                }
            }
            treeList.add(oneTreeMap);
        }
        return treeList;
    }

    @Method(desc = "", logicStep = "")
    private void fieldLegalityValidation(String datasource_name, String datasource_umber, Long[] dep_id) {
        for (long depId : dep_id) {
            if (StringUtil.isBlank(String.valueOf(depId))) {
                throw new BusinessException("部门不能为空或者空格，新增部门时通过主键生成!");
            }
            isExistDepartment(depId);
        }
        Validator.notBlank(datasource_name, "数据源名称不能为空以及不能为空格");
        Matcher matcher = Pattern.compile("^[a-zA-Z]\\w*$").matcher(datasource_umber);
        if (StringUtil.isBlank(datasource_umber) || !matcher.matches()) {
            throw new BusinessException("数据源编号只能是以字母开头的数字、26个英文字母或者下划线组成的字符串:" + datasource_umber);
        }
    }

    @Method(desc = "", logicStep = "")
    private void isExistDataSourceNumber(String datasource_umber) {
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " where datasource_number=? and create_user_id=?", datasource_umber, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("数据源编号已存在:" + datasource_umber);
        }
    }

    @Method(desc = "", logicStep = "")
    private void isExistDataSourceName(String datasource_name) {
        if (Dbo.queryNumber("select count(1) from " + DataSource.TableName + " where datasource_name=? and create_user_id=?", datasource_name, getUserId()).orElseThrow(() -> new BusinessException("sql查询错误！")) > 0) {
            throw new BusinessException("数据源名称已存在:" + datasource_name);
        }
    }

    @Method(desc = "", logicStep = "")
    private void isExistDepartment(long dep_id) {
        if (Dbo.queryNumber("select count(*) from " + DepartmentInfo.TableName + " where dep_id=?", dep_id).orElseThrow(() -> new BusinessException("sql查询错误！")) != 1) {
            throw new BusinessException("该部门ID对应的部门不存在！");
        }
    }

    @ApiOperation(value = "", tags = "")
    private void saveSourceRelationDep(Long source_id, Long[] depIds) {
        for (long depId : depIds) {
            isExistDepartment(depId);
        }
        SourceRelationDep sourceRelationDep = new SourceRelationDep();
        sourceRelationDep.setSource_id(source_id);
        for (long depId : depIds) {
            sourceRelationDep.setDep_id(depId);
            sourceRelationDep.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    private Result getDepNameAndId(long source_id) {
        List<String> depIdList = Dbo.queryOneColumnList("select dep_id from " + SourceRelationDep.TableName + " where source_id=?", source_id);
        if (depIdList.isEmpty()) {
            throw new BusinessException("当前数据源对应部门不存在！");
        }
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DepartmentInfo.TableName + " where ");
        asmSql.addORParam("dep_id", depIdList.toArray());
        return Dbo.queryResult(asmSql.sql().replace("and", " "), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    @Return(desc = "", range = "")
    private void checkAgentField(String agent_ip, String agent_port) {
        Pattern pattern = Pattern.compile("^(\\d|[1-9]\\d|1\\d{2}|2[0-5][0-5])\\.(\\d|[1-9]\\d|1\\d{2}" + "|2[0-5][0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-5][0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-5][0-5])$");
        Matcher matcher = pattern.matcher(agent_ip);
        if (!matcher.matches()) {
            throw new BusinessException("agent_ip不是一个有效的ip地址,agent_ip=" + agent_ip);
        }
        pattern = Pattern.compile("^([0-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]" + "\\d|6553[0-5])$");
        matcher = pattern.matcher(agent_port);
        if (!matcher.matches()) {
            throw new BusinessException("agent_port端口不是有效的端口,agent_port=" + agent_port);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "strTemp", desc = "", range = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "create_user_id", desc = "", range = "")
    private void importDataSource(String strTemp, String agent_ip, String agent_port, long user_id, long create_user_id) {
        Map<String, Object> collectMap = JsonUtil.toObject(strTemp, new TypeReference<Map<String, Object>>() {
        });
        String source_id = getDataSource(create_user_id, collectMap);
        Map<Long, String> departmentInfo = addDepartmentInfo(collectMap);
        addSourceRelationDep(collectMap, source_id, departmentInfo);
        Map<Long, String> agentIdMap = addAgentInfo(agent_ip, agent_port, user_id, source_id, collectMap);
        addAgentDownInfo(agent_ip, agent_port, user_id, collectMap, agentIdMap);
        Map<String, String> classifyAndAgentId = addCollectJobClassify(user_id, collectMap, agentIdMap);
        Map<Long, String> ftpIdMap = addFtpCollect(collectMap, agentIdMap);
        addFtpTransfered(collectMap, ftpIdMap);
        Map<String, String> odcMap = addObjectCollect(collectMap, agentIdMap);
        Map<Long, String> ocsIdMap = addObjectCollectTask(collectMap, odcMap);
        addObjectCollectStruct(collectMap, ocsIdMap);
        Map<Long, String> databaseIdMap = addDatabaseSet(collectMap, classifyAndAgentId);
        Map<String, String> agentAndFcsIdMap = addFileCollectSet(collectMap, agentIdMap);
        addFileSource(collectMap, agentAndFcsIdMap);
        addSignalFile(collectMap, databaseIdMap);
        Map<Long, String> tableIdMap = addTableInfo(collectMap, databaseIdMap);
        addColumnMerge(collectMap, tableIdMap);
        addTableStorageInfo(collectMap, tableIdMap);
        addTableClean(collectMap, tableIdMap);
        Map<Long, String> columnIdMap = addTableColumn(collectMap, tableIdMap);
        Map<String, String> columnAndColIdMap = addColumnClean(collectMap, columnIdMap);
        addColumnSplit(collectMap, columnAndColIdMap);
        addTableCycle(collectMap, tableIdMap);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "tableIdMap", desc = "", range = "")
    private void addTableCycle(Map<String, Object> collectMap, Map<Long, String> tableIdMap) {
        if (collectMap.get(TableCycle.TableName) == null) {
            return;
        }
        List<TableCycle> tableCyclesList = JsonUtil.toObject(collectMap.get(TableCycle.TableName).toString(), new TypeReference<List<TableCycle>>() {
        });
        tableCyclesList.forEach(TableCycle -> {
            TableCycle.setTc_id(PrimayKeyGener.getNextId());
            TableCycle.setTable_id(Long.valueOf(tableIdMap.get(TableCycle.getTable_id())));
            TableCycle.add(Dbo.db());
        });
    }

    @Method(desc = "", logicStep = "")
    private void addTableStorageInfo(Map<String, Object> collectMap, Map<Long, String> tableIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (TableStorageInfo.TableName.equals(entry.getKey())) {
                List<TableStorageInfo> tsiList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<TableStorageInfo>>() {
                });
                if (!tsiList.isEmpty()) {
                    for (TableStorageInfo tableStorageInfo : tsiList) {
                        for (Map.Entry<Long, String> tableIdEntry : tableIdMap.entrySet()) {
                            if (tableIdEntry.getKey().longValue() == tableStorageInfo.getTable_id().longValue()) {
                                tableStorageInfo.setTable_id(Long.valueOf(tableIdEntry.getValue()));
                                tableStorageInfo.setStorage_id(PrimayKeyGener.getNextId());
                                tableStorageInfo.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    private void addColumnMerge(Map<String, Object> collectMap, Map<Long, String> tableIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (ColumnMerge.TableName.equals(entry.getKey())) {
                List<ColumnMerge> columnMergeList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<ColumnMerge>>() {
                });
                if (!columnMergeList.isEmpty()) {
                    for (ColumnMerge columnMerge : columnMergeList) {
                        for (Map.Entry<Long, String> tableIdEntry : tableIdMap.entrySet()) {
                            if (tableIdEntry.getKey().longValue() == columnMerge.getTable_id().longValue()) {
                                columnMerge.setTable_id(Long.valueOf(tableIdEntry.getValue()));
                                columnMerge.setCol_merge_id(PrimayKeyGener.getNextId());
                                columnMerge.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    private void addColumnSplit(Map<String, Object> collectMap, Map<String, String> columnAndColIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (ColumnSplit.TableName.equals(entry.getKey())) {
                List<ColumnSplit> columnSplitList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<ColumnSplit>>() {
                });
                if (!columnSplitList.isEmpty()) {
                    for (ColumnSplit columnSplit : columnSplitList) {
                        for (Map.Entry<String, String> idEntry : columnAndColIdMap.entrySet()) {
                            String[] oldId = idEntry.getKey().split(",");
                            String[] newId = idEntry.getValue().split(",");
                            if (oldId[0].equals(String.valueOf(columnSplit.getColumn_id())) && oldId[1].equals(String.valueOf(columnSplit.getCol_clean_id()))) {
                                columnSplit.setColumn_id(Long.valueOf(newId[0]));
                                columnSplit.setCol_clean_id(Long.valueOf(newId[1]));
                                columnSplit.setCol_split_id(PrimayKeyGener.getNextId());
                                columnSplit.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "columnIdMap", desc = "", range = "")
    private Map<String, String> addColumnClean(Map<String, Object> collectMap, Map<Long, String> columnIdMap) {
        Map<String, String> columnAndColIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (ColumnClean.TableName.equals(entry.getKey())) {
                List<ColumnClean> columnCleanList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<ColumnClean>>() {
                });
                if (!columnCleanList.isEmpty()) {
                    for (ColumnClean columnClean : columnCleanList) {
                        for (Map.Entry<Long, String> columnIdEntry : columnIdMap.entrySet()) {
                            if (columnIdEntry.getKey().longValue() == columnClean.getColumn_id().longValue()) {
                                columnClean.setColumn_id(Long.valueOf(columnIdEntry.getValue()));
                                long colCleanId = PrimayKeyGener.getNextId();
                                columnAndColIdMap.put(columnIdEntry.getKey() + "," + columnClean.getCol_clean_id(), columnIdEntry.getValue() + "," + colCleanId);
                                columnClean.setCol_clean_id(colCleanId);
                                columnClean.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return columnAndColIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "tableIdMap", desc = "", range = "")
    private Map<Long, String> addTableColumn(Map<String, Object> collectMap, Map<Long, String> tableIdMap) {
        Map<Long, String> columnIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (TableColumn.TableName.equals(entry.getKey())) {
                List<TableColumn> tableColumnList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<TableColumn>>() {
                });
                if (!tableColumnList.isEmpty()) {
                    for (TableColumn tableColumn : tableColumnList) {
                        for (Map.Entry<Long, String> tableIdEntry : tableIdMap.entrySet()) {
                            if (tableIdEntry.getKey().longValue() == tableColumn.getTable_id().longValue()) {
                                String column_id = String.valueOf(PrimayKeyGener.getNextId());
                                columnIdMap.put(tableColumn.getColumn_id(), column_id);
                                tableColumn.setColumn_id(Long.valueOf(column_id));
                                tableColumn.setTable_id(Long.valueOf(tableIdEntry.getValue()));
                                tableColumn.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return columnIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "tableIdMap", desc = "", range = "")
    private void addTableClean(Map<String, Object> collectMap, Map<Long, String> tableIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (TableClean.TableName.equals(entry.getKey())) {
                List<TableClean> tableCleanList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<TableClean>>() {
                });
                if (!tableCleanList.isEmpty()) {
                    for (TableClean tableClean : tableCleanList) {
                        for (Map.Entry<Long, String> tableIdEntry : tableIdMap.entrySet()) {
                            if (tableIdEntry.getKey().longValue() == tableClean.getTable_id().longValue()) {
                                tableClean.setTable_id(Long.valueOf(tableIdEntry.getValue()));
                                tableClean.setTable_clean_id(PrimayKeyGener.getNextId());
                                tableClean.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    private Map<Long, String> addTableInfo(Map<String, Object> collectMap, Map<Long, String> databaseIdMap) {
        Map<Long, String> tableIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (TableInfo.TableName.equals(entry.getKey())) {
                List<TableInfo> tableInfoList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<TableInfo>>() {
                });
                if (!tableInfoList.isEmpty()) {
                    for (TableInfo tableInfo : tableInfoList) {
                        for (Map.Entry<Long, String> databaseIdEntry : databaseIdMap.entrySet()) {
                            if (databaseIdEntry.getKey().longValue() == tableInfo.getDatabase_id().longValue()) {
                                tableInfo.setDatabase_id(Long.valueOf(databaseIdEntry.getValue()));
                                String table_id = String.valueOf(PrimayKeyGener.getNextId());
                                tableIdMap.put(tableInfo.getTable_id(), table_id);
                                tableInfo.setTable_id(Long.valueOf(table_id));
                                tableInfo.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return tableIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "databaseIdMap", desc = "", range = "")
    private void addSignalFile(Map<String, Object> collectMap, Map<Long, String> databaseIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (SignalFile.TableName.equals(entry.getKey())) {
                List<SignalFile> signalFileList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<SignalFile>>() {
                });
                if (!signalFileList.isEmpty()) {
                    for (SignalFile signalFile : signalFileList) {
                        for (Map.Entry<Long, String> databaseIdEntry : databaseIdMap.entrySet()) {
                            if (databaseIdEntry.getKey().longValue() == signalFile.getDatabase_id().longValue()) {
                                signalFile.setDatabase_id(Long.valueOf(databaseIdEntry.getValue()));
                                signalFile.setSignal_id(PrimayKeyGener.getNextId());
                                signalFile.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "agentAndFcsIdMap", desc = "", range = "")
    private void addFileSource(Map<String, Object> collectMap, Map<String, String> agentAndFcsIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (FileSource.TableName.equals(entry.getKey())) {
                List<FileSource> fileSourceList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<FileSource>>() {
                });
                if (!fileSourceList.isEmpty()) {
                    for (FileSource fileSource : fileSourceList) {
                        for (Map.Entry<String, String> idEntry : agentAndFcsIdMap.entrySet()) {
                            String[] oldId = idEntry.getKey().split(",");
                            String[] newId = idEntry.getValue().split(",");
                            if (oldId[0].equals(String.valueOf(fileSource.getAgent_id())) && oldId[1].equals(String.valueOf(fileSource.getFcs_id()))) {
                                fileSource.setAgent_id(Long.valueOf(newId[0]));
                                fileSource.setFcs_id(Long.valueOf(newId[1]));
                                fileSource.setFile_source_id(PrimayKeyGener.getNextId());
                                fileSource.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "agentIdMap", desc = "", range = "")
    private Map<String, String> addFileCollectSet(Map<String, Object> collectMap, Map<Long, String> agentIdMap) {
        Map<String, String> agentAndFcsIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (FileCollectSet.TableName.equals(entry.getKey())) {
                List<FileCollectSet> fcsList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<FileCollectSet>>() {
                });
                if (!fcsList.isEmpty()) {
                    for (FileCollectSet FileCollectSet : fcsList) {
                        for (Map.Entry<Long, String> agentIdEntry : agentIdMap.entrySet()) {
                            if (agentIdEntry.getKey().longValue() == FileCollectSet.getAgent_id().longValue()) {
                                long fcs_id = PrimayKeyGener.getNextId();
                                agentAndFcsIdMap.put(agentIdEntry.getKey() + "," + FileCollectSet.getFcs_id(), agentIdEntry.getValue() + "," + fcs_id);
                                FileCollectSet.setFcs_id(fcs_id);
                                FileCollectSet.setAgent_id(Long.valueOf(agentIdEntry.getValue()));
                                FileCollectSet.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return agentAndFcsIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "classifyAndAgentId", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<Long, String> addDatabaseSet(Map<String, Object> collectMap, Map<String, String> classifyAndAgentId) {
        Map<Long, String> databaseIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (DatabaseSet.TableName.equals(entry.getKey())) {
                List<DatabaseSet> databaseSetList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<DatabaseSet>>() {
                });
                if (!databaseSetList.isEmpty()) {
                    for (DatabaseSet DatabaseSet : databaseSetList) {
                        for (Map.Entry<String, String> idEntry : classifyAndAgentId.entrySet()) {
                            String[] oldId = idEntry.getKey().split(",");
                            String[] newId = idEntry.getValue().split(",");
                            if (oldId[0].equals(String.valueOf(DatabaseSet.getAgent_id())) && oldId[1].equals(String.valueOf(DatabaseSet.getClassify_id()))) {
                                DatabaseSet.setAgent_id(Long.valueOf(newId[0]));
                                DatabaseSet.setClassify_id(Long.valueOf(newId[1]));
                                String database_id = String.valueOf(PrimayKeyGener.getNextId());
                                databaseIdMap.put(DatabaseSet.getDatabase_id(), database_id);
                                DatabaseSet.setDatabase_id(Long.valueOf(database_id));
                                DatabaseSet.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return databaseIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "ocsIdMap", desc = "", range = "")
    private void addObjectCollectStruct(Map<String, Object> collectMap, Map<Long, String> ocsIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (ObjectCollectStruct.TableName.equals(entry.getKey())) {
                List<ObjectCollectStruct> ocsList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<ObjectCollectStruct>>() {
                });
                if (!ocsList.isEmpty()) {
                    for (ObjectCollectStruct ObjectCollectStruct : ocsList) {
                        for (Map.Entry<Long, String> ocsIdEntry : ocsIdMap.entrySet()) {
                            if (ocsIdEntry.getKey().longValue() == ObjectCollectStruct.getOcs_id().longValue()) {
                                ObjectCollectStruct.setStruct_id(PrimayKeyGener.getNextId());
                                ObjectCollectStruct.setOcs_id(Long.valueOf(ocsIdEntry.getValue()));
                                ObjectCollectStruct.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "odcMap", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<Long, String> addObjectCollectTask(Map<String, Object> collectMap, Map<String, String> odcMap) {
        Map<Long, String> ocsIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (ObjectCollectTask.TableName.equals(entry.getKey())) {
                List<ObjectCollectTask> octList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<ObjectCollectTask>>() {
                });
                if (!octList.isEmpty()) {
                    for (ObjectCollectTask octTask : octList) {
                        for (Map.Entry<String, String> odcEntry : odcMap.entrySet()) {
                            String[] oldOdcAndAgentId = odcEntry.getKey().split(",");
                            String[] newOdcAndAgentId = odcEntry.getValue().split(",");
                            if (oldOdcAndAgentId[0].equals(String.valueOf(octTask.getOdc_id())) && oldOdcAndAgentId[1].equals(String.valueOf(octTask.getAgent_id()))) {
                                String ocs_id = String.valueOf(PrimayKeyGener.getNextId());
                                ocsIdMap.put(octTask.getOcs_id(), ocs_id);
                                octTask.setOcs_id(Long.valueOf(ocs_id));
                                octTask.setAgent_id(Long.valueOf(newOdcAndAgentId[1]));
                                octTask.setOdc_id(Long.valueOf(newOdcAndAgentId[0]));
                                octTask.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return ocsIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "agentIdMap", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, String> addObjectCollect(Map<String, Object> collectMap, Map<Long, String> agentIdMap) {
        Map<String, String> odcMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (ObjectCollect.TableName.equals(entry.getKey())) {
                List<ObjectCollect> objCollectList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<ObjectCollect>>() {
                });
                if (!objCollectList.isEmpty()) {
                    for (ObjectCollect objCollect : objCollectList) {
                        for (Map.Entry<Long, String> agentIdEntry : agentIdMap.entrySet()) {
                            if (agentIdEntry.getKey().longValue() == objCollect.getAgent_id().longValue()) {
                                long odc_id = PrimayKeyGener.getNextId();
                                odcMap.put(objCollect.getOdc_id() + "," + agentIdEntry.getKey(), odc_id + "," + agentIdEntry.getValue());
                                objCollect.setOdc_id(odc_id);
                                objCollect.setAgent_id(Long.valueOf(agentIdEntry.getValue()));
                                objCollect.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return odcMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "ftpIdMap", desc = "", range = "")
    private void addFtpTransfered(Map<String, Object> collectMap, Map<Long, String> ftpIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (FtpTransfered.TableName.equals(entry.getKey())) {
                List<FtpTransfered> transferList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<FtpTransfered>>() {
                });
                if (!transferList.isEmpty()) {
                    for (FtpTransfered ftp_transfered : transferList) {
                        for (Map.Entry<Long, String> ftpIdEntry : ftpIdMap.entrySet()) {
                            if (ftpIdEntry.getKey().longValue() == ftp_transfered.getFtp_id().longValue()) {
                                ftp_transfered.setFtp_transfered_id(String.valueOf(PrimayKeyGener.getNextId()));
                                ftp_transfered.setFtp_id(Long.valueOf(ftpIdEntry.getValue()));
                                ftp_transfered.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "agentIdMap", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<Long, String> addFtpCollect(Map<String, Object> collectMap, Map<Long, String> agentIdMap) {
        Map<Long, String> ftpIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (FtpCollect.TableName.equals(entry.getKey())) {
                List<FtpCollect> ftpCollectList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<FtpCollect>>() {
                });
                if (!ftpCollectList.isEmpty()) {
                    for (FtpCollect ftp_collect : ftpCollectList) {
                        for (Map.Entry<Long, String> agentIdEntry : agentIdMap.entrySet()) {
                            if (agentIdEntry.getKey().longValue() == ftp_collect.getAgent_id().longValue()) {
                                String ftp_id = String.valueOf(PrimayKeyGener.getNextId());
                                ftpIdMap.put(ftp_collect.getFtp_id(), ftp_id);
                                ftp_collect.setFtp_id(Long.valueOf(ftp_id));
                                ftp_collect.setAgent_id(Long.valueOf(agentIdEntry.getValue()));
                                ftp_collect.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return ftpIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    @Param(name = "userCollectId", desc = "", range = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "agentIdMap", desc = "", range = "")
    private void addAgentDownInfo(String agent_ip, String agent_port, long userCollectId, Map<String, Object> collectMap, Map<Long, String> agentIdMap) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (AgentDownInfo.TableName.equals(entry.getKey())) {
                List<AgentDownInfo> adiList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<AgentDownInfo>>() {
                });
                if (!adiList.isEmpty()) {
                    for (AgentDownInfo agent_down_info : adiList) {
                        for (Map.Entry<Long, String> agentIdEntry : agentIdMap.entrySet()) {
                            if (agent_down_info.getAgent_id().longValue() == agentIdEntry.getKey().longValue()) {
                                agent_down_info.setDown_id(PrimayKeyGener.getNextId());
                                agent_down_info.setUser_id(userCollectId);
                                agent_down_info.setAgent_ip(agent_ip);
                                agent_down_info.setAgent_port(agent_port);
                                agent_down_info.setAgent_id(Long.valueOf(agentIdEntry.getValue()));
                                agent_down_info.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "userCollectId", desc = "", range = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "agentIdMap", desc = "", range = "")
    private Map<String, String> addCollectJobClassify(long userCollectId, Map<String, Object> collectMap, Map<Long, String> agentIdMap) {
        Map<String, String> classifyAndAgentIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (CollectJobClassify.TableName.equals(entry.getKey())) {
                List<CollectJobClassify> cjcList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<CollectJobClassify>>() {
                });
                if (!cjcList.isEmpty()) {
                    for (CollectJobClassify classify : cjcList) {
                        for (Map.Entry<Long, String> agentIdEntry : agentIdMap.entrySet()) {
                            if (classify.getAgent_id().longValue() == agentIdEntry.getKey().longValue()) {
                                long classify_id = PrimayKeyGener.getNextId();
                                classifyAndAgentIdMap.put(agentIdEntry.getKey() + "," + classify.getClassify_id(), agentIdEntry.getValue() + "," + classify_id);
                                classify.setClassify_id(classify_id);
                                classify.setUser_id(userCollectId);
                                classify.setAgent_id(Long.valueOf(agentIdEntry.getValue()));
                                classify.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
        return classifyAndAgentIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<Long, String> addDepartmentInfo(Map<String, Object> collectMap) {
        Map<Long, String> depMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (DepartmentInfo.TableName.equals(entry.getKey())) {
                List<DepartmentInfo> depInfoList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<DepartmentInfo>>() {
                });
                if (!depInfoList.isEmpty()) {
                    for (DepartmentInfo departmentInfo : depInfoList) {
                        Map<String, Object> depInfo = Dbo.queryOneObject("select dep_id,dep_name from " + DepartmentInfo.TableName + " where dep_name=?", departmentInfo.getDep_name());
                        if (depInfo != null && !depInfo.isEmpty()) {
                            depMap.put(departmentInfo.getDep_id(), depInfo.get("dep_id").toString());
                        } else {
                            String dep_id = String.valueOf(PrimayKeyGener.getNextId());
                            depMap.put(departmentInfo.getDep_id(), dep_id);
                            departmentInfo.setDep_id(Long.valueOf(dep_id));
                            departmentInfo.add(Dbo.db());
                        }
                    }
                }
            }
        }
        return depMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "departmentInfo", desc = "", range = "")
    private void addSourceRelationDep(Map<String, Object> collectMap, String source_id, Map<Long, String> departmentInfo) {
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (SourceRelationDep.TableName.equals(entry.getKey())) {
                List<SourceRelationDep> srdList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<SourceRelationDep>>() {
                });
                if (!srdList.isEmpty()) {
                    for (SourceRelationDep SourceRelationDep : srdList) {
                        SourceRelationDep.setSource_id(Long.valueOf(source_id));
                        for (Map.Entry<Long, String> depIdEntry : departmentInfo.entrySet()) {
                            if (depIdEntry.getKey().longValue() == SourceRelationDep.getDep_id().longValue()) {
                                SourceRelationDep.setDep_id(Long.valueOf(depIdEntry.getValue()));
                                SourceRelationDep.add(Dbo.db());
                            }
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "collectMap", desc = "", range = "")
    @Return(desc = "", range = "", isBean = true)
    private String getDataSource(long userId, Map<String, Object> collectMap) {
        String source_id = String.valueOf(PrimayKeyGener.getNextId());
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (DataSource.TableName.equals(entry.getKey())) {
                DataSource DataSource = JsonUtil.toObjectSafety(JsonUtil.toJson(entry.getValue()), DataSource.class).orElseThrow(() -> new BusinessException("json对象转换成实体对象失败！"));
                isExistDataSourceNumber(DataSource.getDatasource_number());
                DataSource.setSource_id(Long.valueOf(source_id));
                DataSource.setCreate_user_id(userId);
                DataSource.add(Dbo.db());
            }
        }
        return source_id;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "agent_ip", desc = "", range = "", example = "")
    @Param(name = "agent_port", desc = "", range = "")
    @Param(name = "userCollectId", desc = "", range = "")
    @Param(name = "source_id", desc = "", range = "")
    @Param(name = "collectMap", desc = "", range = "")
    private Map<Long, String> addAgentInfo(String agent_ip, String agent_port, long userCollectId, String source_id, Map<String, Object> collectMap) {
        Map<Long, String> agentIdMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : collectMap.entrySet()) {
            if (AgentInfo.TableName.equals(entry.getKey())) {
                List<AgentInfo> agentInfoList = JsonUtil.toObject(JsonUtil.toJson(entry.getValue()), new TypeReference<List<AgentInfo>>() {
                });
                if (!agentInfoList.isEmpty()) {
                    for (AgentInfo AgentInfo : agentInfoList) {
                        String agent_id = String.valueOf(PrimayKeyGener.getNextId());
                        agentIdMap.put(AgentInfo.getAgent_id(), agent_id);
                        AgentInfo.setAgent_id(Long.valueOf(agent_id));
                        AgentInfo.setSource_id(Long.valueOf(source_id));
                        AgentInfo.setUser_id(userCollectId);
                        AgentInfo.setAgent_ip(agent_ip);
                        AgentInfo.setAgent_port(agent_port);
                        AgentInfo.add(Dbo.db());
                    }
                }
            }
        }
        return agentIdMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "tableInfoResult", desc = "", range = "")
    private void getTableCycle(Map<String, Object> collectionMap, Result tableInfoResult) {
        if (!tableInfoResult.isEmpty()) {
            SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance().addSql("SELECT * FROM " + TableCycle.TableName).addORParam("table_id", tableInfoResult.toList().stream().map(item -> (Long) item.get("table_id")).toArray(Long[]::new));
            collectionMap.put(TableCycle.TableName, Dbo.queryList(TableCycle.class, assembler.sql(), assembler.params()));
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<AgentInfo> getAgentInfoList(long source_id, Map<String, Object> collectionMap) {
        List<AgentInfo> agentInfoList = Dbo.queryList(AgentInfo.class, "select * from " + AgentInfo.TableName + " where  source_id = ?", source_id);
        collectionMap.put(AgentInfo.TableName, agentInfoList);
        return agentInfoList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "sourceRelationDepList", desc = "", range = "")
    private void addDepartmentInfoToMap(Map<String, Object> collectionMap, List<SourceRelationDep> sourceRelationDepList) {
        List<DepartmentInfo> departmentInfoList = new ArrayList<>();
        for (SourceRelationDep sourceRelationDep : sourceRelationDepList) {
            DepartmentInfo departmentInfo = Dbo.queryOneObject(DepartmentInfo.class, "select * from " + DepartmentInfo.TableName + " where dep_id=?", sourceRelationDep.getDep_id()).orElseThrow(() -> new BusinessException("sql 查询 departmentInfo 信息出错."));
            departmentInfoList.add(departmentInfo);
        }
        collectionMap.put(DepartmentInfo.TableName, departmentInfoList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<SourceRelationDep> addSourceRelationDepToMap(long source_id, Map<String, Object> collectionMap) {
        List<SourceRelationDep> sourceRelationDepList = Dbo.queryList(SourceRelationDep.class, "select * from " + SourceRelationDep.TableName + " where source_id=?", source_id);
        collectionMap.put(SourceRelationDep.TableName, sourceRelationDepList);
        return sourceRelationDepList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "tableColumnResult", desc = "", range = "")
    private void addColumnSplitToMap(Map<String, Object> collectionMap, Result tableColumnResult) {
        Result columnSplitResult = new Result();
        for (int i = 0; i < tableColumnResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + ColumnSplit.TableName + " where column_id = ?", tableColumnResult.getLong(i, "column_id"));
            columnSplitResult.add(result);
        }
        collectionMap.put(ColumnSplit.TableName, columnSplitResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "tableColumnResult", desc = "", range = "")
    private void addColumnCleanToMap(Map<String, Object> collectionMap, Result tableColumnResult) {
        Result columnCleanResult = new Result();
        for (int i = 0; i < tableColumnResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + ColumnClean.TableName + " where column_id=?", tableColumnResult.getLong(i, "column_id"));
            columnCleanResult.add(result);
        }
        collectionMap.put(ColumnClean.TableName, columnCleanResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "tableColumnResult", desc = "", range = "")
    @Return(desc = "", range = "")
    private Result getTableColumnResult(Map<String, Object> collectionMap, Result tableInfoResult) {
        Result tableColumnResult = new Result();
        for (int i = 0; i < tableInfoResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + TableColumn.TableName + " where table_id=?", tableInfoResult.getLong(i, "table_id"));
            tableColumnResult.add(result);
        }
        collectionMap.put(TableColumn.TableName, tableColumnResult.toList());
        return tableColumnResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "tableColumnResult", desc = "", range = "")
    private void addTableCleanToMap(Map<String, Object> collectionMap, Result tableInfoResult) {
        Result tableCleanResult = new Result();
        for (int i = 0; i < tableInfoResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + TableClean.TableName + " where table_id = ?", tableInfoResult.getLong(i, "table_id"));
            tableCleanResult.add(result);
        }
        collectionMap.put(TableClean.TableName, tableCleanResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "tableInfoResult", desc = "", range = "")
    private void addTableStorageInfoToMap(Map<String, Object> collectionMap, Result tableInfoResult) {
        Result tableStorageInfoResult = new Result();
        for (int i = 0; i < tableInfoResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + TableStorageInfo.TableName + " where table_id = ?", tableInfoResult.getLong(i, "table_id"));
            tableStorageInfoResult.add(result);
        }
        collectionMap.put(TableStorageInfo.TableName, tableStorageInfoResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "tableInfoResult", desc = "", range = "")
    private void addColumnMergeToMap(Map<String, Object> collectionMap, Result tableInfoResult) {
        Result columnMergeResult = new Result();
        for (int i = 0; i < tableInfoResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + ColumnMerge.TableName + " where table_id = ?", tableInfoResult.getLong(i, "table_id"));
            columnMergeResult.add(result);
        }
        collectionMap.put(ColumnMerge.TableName, columnMergeResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "databaseSetResult", desc = "", range = "")
    private Result getTableInfoResult(Map<String, Object> collectionMap, Result databaseSetResult) {
        Result tableInfoResult = new Result();
        for (int i = 0; i < databaseSetResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + TableInfo.TableName + " where database_id = ?", databaseSetResult.getLong(i, "database_id"));
            tableInfoResult.add(result);
        }
        collectionMap.put(TableInfo.TableName, tableInfoResult.toList());
        return tableInfoResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "databaseSetResult", desc = "", range = "")
    private void addSignalFileToMap(Map<String, Object> collectionMap, Result databaseSetResult) {
        Result signalFileResult = new Result();
        for (int i = 0; i < databaseSetResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + SignalFile.TableName + " where database_id=?", databaseSetResult.getLong(i, "database_id"));
            signalFileResult.add(result);
        }
        collectionMap.put(SignalFile.TableName, signalFileResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "databaseSetResult", desc = "", range = "")
    private void addFileSourceToMap(Map<String, Object> collectionMap, Result FileCollectSetResult) {
        Result fileSourceResult = new Result();
        for (int i = 0; i < FileCollectSetResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + FileSource.TableName + " where fcs_id=?", FileCollectSetResult.getLong(i, "fcs_id"));
            fileSourceResult.add(result);
        }
        collectionMap.put(FileSource.TableName, fileSourceResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "agentInfoList", desc = "", range = "")
    private Result getFileCollectSetResult(Map<String, Object> collectionMap, List<AgentInfo> agentInfoList) {
        Result fileCollectSetResult = new Result();
        for (AgentInfo AgentInfo : agentInfoList) {
            Result result = Dbo.queryResult("select * from " + FileCollectSet.TableName + " where agent_id=?", AgentInfo.getAgent_id());
            fileCollectSetResult.add(result);
        }
        collectionMap.put(FileCollectSet.TableName, fileCollectSetResult.toList());
        return fileCollectSetResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "agentInfoList", desc = "", range = "")
    private Result getDatabaseSetResult(Map<String, Object> collectionMap, List<AgentInfo> agentInfoList) {
        Result databaseSetResult = new Result();
        for (AgentInfo AgentInfo : agentInfoList) {
            Result result = Dbo.queryResult("select * from " + DatabaseSet.TableName + " where agent_id = ?", AgentInfo.getAgent_id());
            databaseSetResult.add(result);
        }
        collectionMap.put(DatabaseSet.TableName, databaseSetResult.toList());
        return databaseSetResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "objectCollectTaskResult", desc = "", range = "")
    private void addObjectCollectStructResultToMap(Map<String, Object> collectionMap, Result objectCollectTaskResult) {
        Result objectCollectStructResult = new Result();
        for (int i = 0; i < objectCollectTaskResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + ObjectCollectStruct.TableName + " where ocs_id =?", objectCollectTaskResult.getLong(i, "ocs_id"));
            objectCollectStructResult.add(result);
        }
        collectionMap.put(ObjectCollectStruct.TableName, objectCollectStructResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "objectCollectResult", desc = "", range = "")
    @Return(desc = "", range = "")
    private Result getObjectCollectTaskResult(Map<String, Object> collectionMap, Result objectCollectResult) {
        Result objectCollectTaskResult = new Result();
        for (int i = 0; i < objectCollectResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + ObjectCollectTask.TableName + " where odc_id=?", objectCollectResult.getLong(i, "odc_id"));
            objectCollectTaskResult.add(result);
        }
        collectionMap.put(ObjectCollectTask.TableName, objectCollectTaskResult.toList());
        return objectCollectTaskResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "agentInfoList", desc = "", range = "")
    @Return(desc = "", range = "")
    private Result getObjectCollectResult(Map<String, Object> collectionMap, List<AgentInfo> agentInfoList) {
        Result objectCollectResult = new Result();
        for (AgentInfo AgentInfo : agentInfoList) {
            Result ObjectCollect = Dbo.queryResult("select * from " + hyren.serv6.base.entity.ObjectCollect.TableName + " where agent_id=?", AgentInfo.getAgent_id());
            objectCollectResult.add(ObjectCollect);
        }
        collectionMap.put(ObjectCollect.TableName, objectCollectResult.toList());
        return objectCollectResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "agentInfoList", desc = "", range = "")
    private void addAgentDownInfoToMap(Map<String, Object> collectionMap, List<AgentInfo> agentInfoList) {
        List<AgentDownInfo> agentDownInfoList = new ArrayList<>();
        for (AgentInfo AgentInfo : agentInfoList) {
            Optional<AgentDownInfo> agent_down_info = Dbo.queryOneObject(AgentDownInfo.class, "select * from  " + AgentDownInfo.TableName + " where  agent_id = ?", AgentInfo.getAgent_id());
            agent_down_info.ifPresent(agentDownInfoList::add);
        }
        collectionMap.put(AgentDownInfo.TableName, agentDownInfoList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceId", desc = "", range = "")
    @Param(name = "collectionMap", desc = "", range = "")
    private void addDataSourceToMap(long source_id, Map<String, Object> collectionMap) {
        DataSource dataSource = Dbo.queryOneObject(DataSource.class, "select * from " + DataSource.TableName + " where source_id = ?", source_id).orElseThrow(() -> new BusinessException("此数据源下没有数据，sourceId = ?" + source_id));
        collectionMap.put(DataSource.TableName, dataSource);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "ftpCollectResult", desc = "", range = "")
    private void addFtpTransferedToMap(Map<String, Object> collectionMap, Result ftpCollectResult) {
        Result ftpTransferedResult = new Result();
        for (int i = 0; i < ftpCollectResult.getRowCount(); i++) {
            Result result = Dbo.queryResult("select * from " + FtpTransfered.TableName + " where ftp_id=?", ftpCollectResult.getLong(i, "ftp_id"));
            ftpTransferedResult.add(result);
        }
        collectionMap.put(FtpTransfered.TableName, ftpTransferedResult.toList());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "agentInfoList", desc = "", range = "")
    @Return(desc = "", range = "")
    private Result getFtpCollectResult(Map<String, Object> collectionMap, List<AgentInfo> agentInfoList) {
        Result ftpCollectResult = new Result();
        for (AgentInfo AgentInfo : agentInfoList) {
            Result result = Dbo.queryResult("select * from " + FtpCollect.TableName + " where agent_id = ?", AgentInfo.getAgent_id());
            ftpCollectResult.add(result);
        }
        collectionMap.put(FtpCollect.TableName, ftpCollectResult.toList());
        return ftpCollectResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "collectionMap", desc = "", range = "")
    @Param(name = "agentInfoList", desc = "", range = "")
    private void addCollectJobClassifyToMap(Map<String, Object> collectionMap, List<AgentInfo> agentInfoList) {
        Result collectJobClassifyResult = new Result();
        for (AgentInfo AgentInfo : agentInfoList) {
            Result result = Dbo.queryResult("select * from " + CollectJobClassify.TableName + " where agent_id=?", AgentInfo.getAgent_id());
            collectJobClassifyResult.add(result);
        }
        collectionMap.put(CollectJobClassify.TableName, collectJobClassifyResult.toList());
    }
}
