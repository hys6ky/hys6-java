package hyren.serv6.b.fulltextsearch;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.b.fulltextsearch.tools.PictureSearch;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.utils.RequestUtil;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.constant.PathUtil;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;
import java.util.*;

@Slf4j
@Service
public class FullTextSearchService {

    @Method(desc = "", logicStep = "")
    @Param(name = "queryNum", desc = "", range = "", valueIfNull = "9")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getCollectFiles(int queryNum) {
        queryNum = Math.max(1, queryNum);
        queryNum = Math.min(queryNum, 99);
        Page page = new DefaultPageImpl(1, queryNum);
        return Dbo.queryPagedList(page, "SELECT uf.*,sfa.file_suffix,sfa.hbase_name,sfa.file_type FROM " + UserFav.TableName + " uf" + " JOIN " + SourceFileAttribute.TableName + " sfa ON sfa.file_id = uf.file_id" + " WHERE fav_flag = ?" + " ORDER BY fav_id DESC", IsFlag.Shi.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fullTextSearchMethod", desc = "", range = "", valueIfNull = "unstructuredFileSearch")
    @Param(name = "queryKeyword", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> fullTextSearch(String fullTextSearchMethod, String queryKeyword, int currPage, int pageSize) {
        Validator.notNull(queryKeyword, "检索内容不能为空");
        Map<String, Object> searchResultMap = new HashMap<>();
        try {
            String participles = processingParticiples(queryKeyword.trim());
            switch(fullTextSearchMethod) {
                case "fullTextSearch":
                    searchResultMap.put("unstructuredhRs", unstructuredFileSearch(participles, currPage, pageSize));
                    searchResultMap.put("structuredRs", structuredFileSearch(participles, currPage, pageSize));
                    break;
                case "unstructuredFileSearch":
                    searchResultMap.put("unstructuredhRs", unstructuredFileSearch(participles, currPage, pageSize));
                    break;
                case "structuredFileSearch":
                    searchResultMap.put("structuredRs", structuredFileSearch(participles, currPage, pageSize));
                    break;
            }
        } catch (Exception e) {
            throw new BusinessException("获取solr操作实例失败！ e:" + e);
        }
        searchResultMap.put("analysis", Arrays.asList(queryKeyword.split(Constant.SPACE)));
        return searchResultMap;
    }

    private List<Map<String, Object>> unstructuredFileSearch(String queryKeyword, int currPage, int pageSize) {
        Result result = getUnstructuredFinalResult(queryKeyword, currPage, pageSize);
        return unstructuredFileResultProcessing(result);
    }

    private List<Map<String, Object>> structuredFileSearch(String queryKeyword, int currPage, int pageSize) {
        Result structuredFileSearchRs = getStructuredFinalResult(queryKeyword, currPage, pageSize);
        List<Map<String, Object>> rList = new ArrayList<>();
        if (!structuredFileSearchRs.isEmpty()) {
            for (Map<String, Object> stringObjectMap : structuredFileSearchRs.toList()) {
                stringObjectMap.put("collectTime", DateUtil.parseStr2DateWith8Char((String) stringObjectMap.get("storage_date")) + " " + DateUtil.parseStr2TimeWith6Char((String) stringObjectMap.get("storage_time")));
                rList.add(stringObjectMap);
            }
        }
        return rList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "imageAddress", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Map<String, Object> searchByMap(String imageAddress) {
        Map<String, Object> resultMap = new HashMap<>();
        PictureSearch picSearch = new PictureSearch();
        Result result = picSearch.pictureSearchResult(imageAddress);
        resultMap.put("result", result);
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "docAddress", desc = "", range = "", nullable = true)
    @Param(name = "similarityRate", desc = "", range = "", valueIfNull = "0")
    @Param(name = "searchWay", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> articleSimilarityQuery(String docAddress, String similarityRate, String searchWay) {
        IsFlag searchWayFlag = IsFlag.ofEnumByCode(searchWay);
        Result result = getWZXSDResult(docAddress, similarityRate, searchWayFlag);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("analysis", new ArrayList<>());
        resultMap.put("unstructuredhRs", unstructuredFileResultProcessing(result));
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "fileName", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> fileNameSearch(int currPage, int pageSize, String fileName) {
        Result result = getWJMSSResult(fileName, currPage, pageSize);
        List<String> analysis = new ArrayList<>();
        analysis.add(fileName);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("analysis", analysis);
        resultMap.put("unstructuredhRs", unstructuredFileResultProcessing(result));
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "result", desc = "", range = "")
    @Return(desc = "", range = "")
    private List<Map<String, Object>> unstructuredFileResultProcessing(Result result) {
        List<Map<String, Object>> rList = new ArrayList<>();
        if (!result.isEmpty()) {
            String requestUrl = RequestUtil.getRequest().getRequestURL().toString();
            String action = requestUrl.substring(0, requestUrl.lastIndexOf('/') + 1);
            for (Map<String, Object> stringObjectMap : result.toList()) {
                String fileId = (String) stringObjectMap.get("file_id");
                String originalName = (String) stringObjectMap.get("original_name");
                String downloadPath = action + "downloadFile?fileId=" + fileId + "&originalName=" + originalName;
                stringObjectMap.put("downloadPath", downloadPath);
                stringObjectMap.put("collectTime", DateUtil.parseStr2DateWith8Char((String) stringObjectMap.get("storage_date")) + " " + DateUtil.parseStr2TimeWith6Char((String) stringObjectMap.get("storage_time")));
                rList.add(stringObjectMap);
            }
        }
        return rList;
    }

    private Result getUnstructuredFinalResult(String queryConditions, int currPage, int pageSize) {
        Result resultProcessing = unStructuredQueryResultProcessing(queryConditions, currPage, pageSize);
        if (resultProcessing.isEmpty())
            return new Result();
        List<Long> agentIdList = new ArrayList<>();
        List<String> fileIdList = new ArrayList<>();
        for (int i = 0; i < resultProcessing.getRowCount(); i++) {
            long agentId = resultProcessing.getLong(i, "agent_id");
            String fileId = resultProcessing.getString(i, "file_id");
            agentIdList.add(agentId);
            fileIdList.add(fileId);
        }
        Object[] newAgentIdArr = agentIdList.stream().distinct().toArray();
        Object[] newFileIdArr = fileIdList.toArray();
        SearchInfo searchInfo = new SearchInfo();
        searchInfo.setWord_name(queryConditions);
        SourceFileAttribute sourceFileAttribute = new SourceFileAttribute();
        sourceFileAttribute.setCollect_type(AgentType.WenJianXiTong.getCode());
        StringBuilder sb = new StringBuilder();
        sb.append(" SELECT sfa.source_path,sfa.file_suffix,sfa.file_id,sfa.storage_time,sfa.storage_date," + " sfa.original_update_date,sfa.hbase_name, sfa.original_update_time,sfa.file_md5,sfa.original_name," + " sfa.file_size,sfa.seqencing,collect_type,sfa.collect_set_id,sfa.source_id,sfa.agent_id," + " fcs.fcs_name,datasource_name,agent_name,si.si_count,uf.fav_id,uf.fav_flag,sfa.file_type" + " FROM " + DataSource.TableName + " ds" + " JOIN " + AgentInfo.TableName + " gi ON gi.SOURCE_ID = ds.SOURCE_ID" + " JOIN " + FileCollectSet.TableName + " fcs ON fcs.agent_id = gi.agent_id" + " JOIN " + SourceFileAttribute.TableName + " sfa ON sfa.SOURCE_ID = ds.SOURCE_ID and sfa.AGENT_ID = gi.AGENT_ID" + " and sfa.COLLECT_SET_ID = fcs.FCS_ID " + "LEFT JOIN " + SearchInfo.TableName + " si ON word_name = '").append(searchInfo.getWord_name()).append("'").append(" LEFT JOIN " + UserFav.TableName + " uf ON sfa.file_id=uf.file_id").append(" WHERE collect_type = '").append(sourceFileAttribute.getCollect_type()).append("'");
        if (!agentIdList.isEmpty()) {
            sb.append(" AND sfa.agent_id in (");
            for (Object agentId : newAgentIdArr) {
                sb.append(agentId).append(",");
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append(")");
        }
        if (!fileIdList.isEmpty()) {
            sb.append(" AND sfa.file_id in (");
            for (Object fileId : newFileIdArr) {
                sb.append("'").append(fileId).append("'").append(",");
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append(")");
        }
        DefaultPageImpl page = new DefaultPageImpl(currPage, pageSize);
        Result resultSet = Dbo.queryPagedResult(page, sb.toString());
        for (int i = 0; i < resultSet.getRowCount(); i++) {
            String fileId = resultSet.getString(i, "file_id");
            for (int j = 0; j < resultProcessing.getRowCount(); j++) {
                String fileIdSolr = resultProcessing.getString(j, "file_id");
                if (fileId.equals(fileIdSolr)) {
                    resultSet.setObject(i, "summary_content", resultProcessing.getString(j, "summary_content"));
                }
            }
        }
        return resultSet;
    }

    private Result getStructuredFinalResult(String queryConditions, int currPage, int pageSize) {
        Result resultProcessing = structuredQueryResultProcessing(queryConditions, currPage, pageSize);
        if (resultProcessing.isEmpty())
            return new Result();
        List<Long> agentIdList = new ArrayList<>();
        List<String> fileIdList = new ArrayList<>();
        for (int i = 0; i < resultProcessing.getRowCount(); i++) {
            long agentId = resultProcessing.getLong(i, "agent_id");
            String fileId = resultProcessing.getString(i, "file_id");
            agentIdList.add(agentId);
            fileIdList.add(fileId);
        }
        Object[] newAgentIdArr = agentIdList.stream().distinct().toArray();
        Object[] newFileIdArr = fileIdList.toArray();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql(" select dsr.* FROM " + DataSource.TableName + " ds" + " JOIN " + AgentInfo.TableName + " ai ON ai.SOURCE_ID = ds.SOURCE_ID" + " JOIN " + DatabaseSet.TableName + " dbs ON dbs.agent_id = ai.agent_id" + " JOIN " + DataStoreReg.TableName + " dsr ON dsr.SOURCE_ID = ds.SOURCE_ID" + " and dsr.AGENT_ID = ai.AGENT_ID" + " and dsr.database_id = dbs.database_id");
        asmSql.addSql(" where dsr.collect_type in (?,?) ").addParam(AgentType.ShuJuKu.getCode()).addParam(AgentType.DBWenJian.getCode());
        asmSql.addORParam("dsr.AGENT_ID", newAgentIdArr);
        asmSql.addORParam("dsr.file_id", newFileIdArr);
        DefaultPageImpl page = new DefaultPageImpl(currPage, pageSize);
        Result resultSet = Dbo.queryPagedResult(page, asmSql.sql(), asmSql.params());
        for (int i = 0; i < resultSet.getRowCount(); i++) {
            String fileId = resultSet.getString(i, "file_id");
            for (int j = 0; j < resultProcessing.getRowCount(); j++) {
                String fileIdSolr = resultProcessing.getString(j, "file_id");
                if (fileId.equals(fileIdSolr)) {
                    resultSet.setObject(i, "summary_content", resultProcessing.getString(j, "summary_content"));
                    resultSet.setObject(i, "csv", resultProcessing.getObject(j, "csv"));
                }
            }
        }
        return resultSet;
    }

    private String processingParticiples(String queryKeyword) {
        List<String> participleList = Arrays.asList(queryKeyword.split(Constant.SPACE));
        StringBuilder queryPlus = new StringBuilder();
        for (int i = 0; i < participleList.size(); i++) {
            String s = participleList.get(i);
            if (StringUtil.isNotBlank(s)) {
                queryPlus.append("*").append(s).append("*");
                if (i != participleList.size() - 1) {
                    queryPlus.append(Constant.SPACE).append("OR").append(Constant.SPACE);
                }
            }
        }
        log.info("finalQueryPlus: " + queryPlus);
        return queryPlus.toString();
    }

    private Result unStructuredQueryResultProcessing(String queryConditions, int currPage, int pageSize) {
        try (ISolrOperator os = SolrFactory.getSolrOperatorInstance()) {
            Result processingResult = new Result();
            int start = 0;
            while (true) {
                List<Map<String, Object>> querySolrRs = getQueryFromSolr(os, queryConditions, start, CommonVariables.FULL_TEXT_SEARCH_BATCH_NUM);
                start += CommonVariables.FULL_TEXT_SEARCH_BATCH_NUM;
                if (querySolrRs.isEmpty())
                    return processingResult;
                Map<String, String> unstructuredSearchMap = new HashMap<>();
                for (Map<String, Object> parseObject : querySolrRs) {
                    unstructuredSearchMap.put(parseObject.get("id").toString(), null == parseObject.get("file_summary") ? "" : parseObject.get("file_summary").toString());
                }
                HashSet<String> idList = new HashSet<>(unstructuredSearchMap.keySet());
                if (!idList.isEmpty()) {
                    SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
                    asmSql.clean();
                    asmSql.addSql("select * from " + SourceFileAttribute.TableName).addORParam("file_id", idList.toArray()).addSql(" ORDER BY file_id");
                    Result sfa_rs = Dbo.queryResult(asmSql.sql(), asmSql.params());
                    for (int i = 0; i < sfa_rs.getRowCount(); i++) {
                        sfa_rs.setObject(i, "summary_content", unstructuredSearchMap.get(sfa_rs.getString(i, "file_id")));
                    }
                    processingResult.add(sfa_rs);
                    if (processingResult.getRowCount() >= currPage * pageSize) {
                        return processingResult;
                    }
                }
            }
        } catch (Exception e) {
            throw new BusinessException("Solr数据关联表处理发生异常! " + e);
        }
    }

    private Result structuredQueryResultProcessing(String queryConditions, int currPage, int pageSize) {
        Result processingResult = new Result();
        List<DataStoreLayer> dsls = Dbo.queryList(DataStoreLayer.class, "SELECT * from " + DataStoreLayer.TableName + " WHERE store_type=?", Store_type.SOLR.getCode());
        for (DataStoreLayer dsl : dsls) {
            Map<String, String> layerMap = ConnectionTool.getLayerMap(Dbo.db(), dsl.getDsl_id());
            SolrParam solrParam = new SolrParam();
            solrParam.setSolrZkUrl(layerMap.get(StorageTypeKey.solr_zk_url));
            solrParam.setCollection(layerMap.get(StorageTypeKey.collection));
            try (ISolrOperator os = SolrFactory.getSolrOperatorInstance(solrParam)) {
                int start = 0;
                while (true) {
                    List<Map<String, Object>> querySolrRs = getQueryFromSolr(os, queryConditions, start, 1000);
                    start += CommonVariables.FULL_TEXT_SEARCH_BATCH_NUM;
                    if (querySolrRs.isEmpty())
                        return processingResult;
                    Map<String, List<Map<String, Object>>> mapCsvList = new HashMap<>();
                    HashSet<String> tableNameList = new HashSet<>();
                    for (Map<String, Object> rsMap : querySolrRs) {
                        for (Map.Entry<String, Object> rsEntry : rsMap.entrySet()) {
                            String value = rsEntry.getValue().toString();
                            if (value.startsWith("[")) {
                                value = value.substring(1);
                            }
                            if (value.endsWith("]")) {
                                value = value.substring(0, value.length() - 1);
                            }
                            rsEntry.setValue(value);
                        }
                        String tableName = rsMap.get("table-name") == null ? "" : rsMap.get("table-name").toString();
                        if (StringUtil.isNotBlank(tableName)) {
                            if (tableName.startsWith("[")) {
                                tableName = tableName.substring(1);
                            }
                            if (tableName.endsWith("]")) {
                                tableName = tableName.substring(0, tableName.length() - 1);
                            }
                            tableNameList.add(tableName);
                            rsMap.put("table_name", tableName);
                            rsMap.remove("table-name");
                            if (mapCsvList.containsKey(tableName)) {
                                mapCsvList.get(tableName).add(rsMap);
                            } else {
                                List<Map<String, Object>> mapCsvs = new ArrayList<>();
                                mapCsvs.add(rsMap);
                                mapCsvList.put(tableName, mapCsvs);
                            }
                        }
                    }
                    if (tableNameList.isEmpty()) {
                        throw new BusinessException("未找到检索结果登记表信息!");
                    } else {
                        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
                        asmSql.clean();
                        asmSql.addSql("SELECT * FROM " + DataStoreReg.TableName).addORParam("hyren_name", tableNameList.toArray()).addSql(" ORDER BY hyren_name");
                        Result dsr_rs = Dbo.queryResult(asmSql.sql(), asmSql.params());
                        for (int i = 0; i < dsr_rs.getRowCount(); i++) {
                            dsr_rs.setObject(i, "csv", mapCsvList.get(dsr_rs.getString(i, "hyren_name")));
                        }
                        processingResult.add(dsr_rs);
                        if (processingResult.getRowCount() >= currPage * pageSize || querySolrRs.size() <= pageSize) {
                            return processingResult;
                        }
                    }
                }
            } catch (Exception e) {
                throw new BusinessException("创建solr连接失败! " + " solr_zk_url: " + solrParam.getSolrZkUrl() + " collection: " + solrParam.getCollection() + " ,e:" + e);
            }
        }
        return processingResult;
    }

    @Method(desc = "", logicStep = "")
    private List<Map<String, Object>> getQueryFromSolr(ISolrOperator os, String queryConditions, int start, int rows) {
        if (StringUtil.isEmpty(queryConditions)) {
            queryConditions = "*:*";
        }
        return os.querySolr(queryConditions, start, rows);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "docAddress", desc = "", range = "")
    @Param(name = "similarityRate", desc = "", range = "")
    @Param(name = "searchWayFlag", desc = "", range = "")
    @Return(desc = "", range = "")
    private Result getWZXSDResult(String docAddress, String similarityRate, IsFlag searchWayFlag) {
        Result filterQueryRs = new Result();
        if (StringUtil.isEmpty(similarityRate)) {
            similarityRate = "0";
        }
        List<Map<String, String>> documentSimilarList = ClassBase.essaySimilar().getDocumentSimilarFromSolr(docAddress, similarityRate, searchWayFlag);
        documentSimilarList.forEach(documentSimilar -> {
            if (filterQueryRs.getRowCount() == 10)
                return;
            String file_id = documentSimilar.get("file_id");
            SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
            asmSql.clean();
            asmSql.addSql(" SELECT sfa.*,ds.datasource_name,gi.agent_name,fcs.fcs_name,uf.fav_id,uf.fav_flag FROM");
            asmSql.addSql(" data_source ds  JOIN agent_info gi ON ds.source_id = gi.source_id");
            asmSql.addSql(" JOIN file_collect_set fcs ON fcs.agent_id = gi.agent_id");
            asmSql.addSql(" JOIN source_file_attribute sfa ON sfa.source_id = ds.source_id");
            asmSql.addSql(" and  sfa.agent_id = gi.agent_id");
            asmSql.addSql(" and sfa.collect_set_id = fcs.fcs_id");
            asmSql.addSql(" LEFT JOIN user_fav uf ON sfa.file_id = uf.file_id");
            asmSql.addSql(" where sfa.file_id = ?");
            asmSql.addSql(" and collect_type = ? ORDER BY  sfa.file_id");
            asmSql.addParam(file_id);
            asmSql.addParam(AgentType.WenJianXiTong.getCode());
            Result result = Dbo.queryResult(asmSql.sql(), asmSql.params());
            if (!result.isEmpty()) {
                result.setObject(0, "summary_content", documentSimilar.get("summary_content"));
                result.setObject(0, "rate", documentSimilar.get("rate"));
                result.setObject(0, "totalSize", documentSimilarList.size());
                filterQueryRs.add(result);
            }
        });
        filterQueryRs.sortResult("rate", "desc");
        return filterQueryRs;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    private Result getWJMSSResult(String fileName, @RequestParam(defaultValue = "1") int currPage, @RequestParam(defaultValue = "10") int pageSize) {
        Object[] sourceIdsObj = Dbo.queryOneColumnList("select source_id from data_source").toArray();
        fileName = fileName.trim();
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("SELECT sfa.*,ds.datasource_name,gi.agent_name,fcs.fcs_name,uf.fav_id,uf.fav_flag from (" + " SELECT a.* FROM source_file_attribute a  WHERE collect_type = ? ");
        asmSql.addParam(AgentType.WenJianXiTong.getCode());
        asmSql.addLikeParam("original_name", "%" + fileName + "%");
        asmSql.addORParam("a.source_id", sourceIdsObj);
        asmSql.addSql(" ) sfa join data_source ds  ON sfa.source_id=ds.source_id JOIN agent_info gi ON sfa.agent_id =" + " gi.agent_id JOIN file_collect_set fcs ON sfa.collect_set_id = fcs.fcs_id LEFT JOIN user_fav uf ON" + " sfa.file_id = uf.file_id ORDER BY seqencing DESC");
        DefaultPageImpl page = new DefaultPageImpl(currPage, pageSize);
        Result fileNameSearchResult = Dbo.queryPagedResult(page, asmSql.sql(), asmSql.params());
        for (int i = 0; i < fileNameSearchResult.getRowCount(); i++) {
            String fileAvroPath = fileNameSearchResult.getString(i, "file_avro_path");
            fileAvroPath = PathUtil.convertLocalPathToHDFSPath(fileAvroPath);
            String fileAvroBlock = fileNameSearchResult.getString(i, "file_avro_block");
            String fileId = fileNameSearchResult.getString(i, "file_id");
            String fileSummary = ClassBase.essaySimilar().getFileSummaryFromAvro(fileAvroPath, fileAvroBlock, fileId);
            fileNameSearchResult.setObject(i, "summary_content", fileSummary);
        }
        return fileNameSearchResult;
    }
}
