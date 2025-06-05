package hyren.serv6.commons.solr.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.FieldAnalysisRequest;
import org.apache.solr.client.solrj.response.AnalysisResponseBase.AnalysisPhase;
import org.apache.solr.client.solrj.response.AnalysisResponseBase.TokenInfo;
import org.apache.solr.client.solrj.response.FieldAnalysisResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import java.io.IOException;
import java.util.*;

@Setter
@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/14 0014 下午 05:02")
@Slf4j
public abstract class AbsSolrOperatorImpl implements ISolrOperator {

    protected SolrClient solrClient;

    protected static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";

    public AbsSolrOperatorImpl() {
    }

    @Override
    public SolrClient getSolrClient() {
        return solrClient;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sentence", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    @Override
    public List<String> getAnalysis(String sentence) {
        List<String> words = new ArrayList<>();
        try {
            FieldAnalysisRequest request = new FieldAnalysisRequest("/analysis/field");
            request.addFieldName("_root_");
            request.setFieldValue("");
            request.setQuery(sentence);
            FieldAnalysisResponse response = request.process(solrClient);
            for (AnalysisPhase phase : response.getFieldNameAnalysis("_root_").getQueryPhases()) {
                List<TokenInfo> list = phase.getTokens();
                for (TokenInfo info : list) {
                    if (!info.getText().equals(" ")) {
                        words.add(info.getText());
                    }
                }
            }
        } catch (Exception e) {
            String message = String.format("获取字段分词解析失败:%s", e);
            log.error(message);
            throw new AppSystemException(message);
        }
        return words;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "searchCondition", desc = "", range = "")
    @Param(name = "start", desc = "", range = "")
    @Param(name = "rows", desc = "", range = "")
    @Return(desc = "", range = "")
    @Override
    public List<Map<String, Object>> querySolr(String searchCondition, int start, int rows) {
        Map<String, String> solrParams = new HashMap<>();
        solrParams.put("q", searchCondition);
        solrParams.put("qt", "/select");
        solrParams.put("wt", "json");
        solrParams.put("indent", "true");
        return querySolrPlus(solrParams, start, rows, IsFlag.Shi);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "solrParams", desc = "", range = "")
    @Param(name = "start", desc = "", range = "")
    @Param(name = "rows", desc = "", range = "")
    @Param(name = "flag", desc = "", range = "")
    @Return(desc = "", range = "")
    @Override
    public List<Map<String, Object>> querySolrPlus(Map<String, String> solrParams, int start, int rows, IsFlag is_return_file_text) {
        List<Map<String, Object>> solrDocList = new ArrayList<>();
        try {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.set("q", solrParams.get("q"));
            if (0 != rows) {
                solrQuery.setStart(start);
                solrQuery.setRows(rows);
            }
            solrQuery.set("fq", solrParams.get("fq"));
            solrQuery.set("wt", solrParams.get("wt"));
            solrQuery.set("indent", solrParams.get("indent"));
            solrQuery.set("sort", solrParams.get("sort"));
            solrQuery.set("fl", solrParams.get("fl"));
            solrQuery.set("df", "tf-file_text");
            solrQuery.setIncludeScore(true);
            QueryResponse queryResponse = solrClient.query(solrQuery);
            SolrDocumentList solrDocumentList = queryResponse.getResults();
            log.debug("检索到的所有记录数: " + queryResponse.getResults().getNumFound() + " 条");
            for (SolrDocument singleDoc : solrDocumentList) {
                Map<String, Object> resMap = new HashMap<>();
                resMap.put("score", singleDoc.getFieldValue("score"));
                String sub_field;
                for (String fieldName : singleDoc.getFieldNames()) {
                    if (!fieldName.equals("_version_") && !fieldName.equals("score")) {
                        if (is_return_file_text == IsFlag.Fou && fieldName.equals("tf-file_text")) {
                            continue;
                        }
                        if (!fieldName.equals("id") && !fieldName.equals("table-name")) {
                            sub_field = fieldName.substring(3).trim();
                        } else {
                            sub_field = fieldName;
                        }
                        resMap.put(sub_field, singleDoc.getFieldValue(fieldName));
                    }
                }
                solrDocList.add(resMap);
            }
        } catch (Exception e) {
            log.error("获取solr检索结果失败!");
            throw new BusinessException("获取solr检索结果失败:" + e.getMessage());
        }
        return solrDocList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "id", desc = "", range = "")
    @Override
    public void deleteIndexById(String id) {
        try {
            solrClient.deleteById(id);
            solrClient.commit();
        } catch (Exception e) {
            log.error("根据id删除索引失败! id=" + id);
            throw new AppSystemException("根据id删除索引失败! id=" + id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "query", desc = "", range = "")
    @Override
    public void deleteIndexByQuery(String query) {
        try {
            solrClient.deleteByQuery(query);
            solrClient.commit();
        } catch (Exception e) {
            log.error("根据查询语句删除索引失败! query=" + query);
            throw new AppSystemException("根据查询语句删除索引失败! query=" + query);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "参数名", desc = "", range = "")
    @Return(desc = "", range = "")
    @Override
    public void deleteIndexAll() {
        try {
            solrClient.deleteByQuery("*:*");
            solrClient.commit();
        } catch (Exception e) {
            log.error("全索引删除索引失败!");
            throw new AppSystemException("全索引删除索引失败!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "is", desc = "", range = "")
    @Param(name = "params", desc = "", range = "")
    @Param(name = "handler", desc = "", range = "")
    @Return(desc = "", range = "")
    @Override
    public List<Map<String, Object>> requestHandler(boolean is, Map<String, Object> params, String... handler) {
        handler = handler.length < 1 ? new String[] { Constant.HANDLER } : handler;
        if (!is) {
            return requestHandler(handler);
        }
        SolrQuery sq = new SolrQuery();
        if (!params.isEmpty()) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                sq.set(entry.getKey(), entry.getValue().toString());
            }
        }
        List<Map<String, Object>> handlerList = new ArrayList<>();
        Map<String, Object> handlerMap = new HashMap<>();
        QueryResponse response = new QueryResponse();
        for (String h : handler) {
            sq.setRequestHandler(h);
            try {
                response = solrClient.query(sq);
            } catch (SolrServerException | IOException e) {
                handlerMap.put(h, e);
                throw new AppSystemException("获取自定义的handler,(带返回值)时发生异常,查看日志进行处理!" + e);
            }
            handlerMap.put(h, response.getResponse());
            handlerList.add(handlerMap);
            log.info("spend time on request to custom handler " + h + ":" + response.getQTime() + " ms");
        }
        return handlerList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fieldName", desc = "", range = "")
    @Param(name = "filedValue", desc = "", range = "")
    @Param(name = "fieldName", desc = "", range = "")
    @Return(desc = "", range = "")
    @Override
    public SolrDocumentList queryByField(String fieldName, String filedValue, String responseColumns, int rows) {
        SolrDocumentList solrDocuments;
        try {
            SolrQuery solrQuery = new SolrQuery();
            solrQuery.set("q", fieldName + ":" + filedValue);
            solrQuery.set("fl", responseColumns);
            solrQuery.set("qt", "/select");
            solrQuery.set("wt", "json");
            solrQuery.set("indent", "true");
            solrQuery.setRows(rows);
            solrDocuments = solrClient.query(solrQuery).getResults();
        } catch (SolrServerException | IOException e) {
            log.error(e.getMessage(), e);
            throw new AppSystemException("SolrServer服务异常!");
        }
        return solrDocuments;
    }

    @Override
    public void parseResultToSolr(Result result, Set<String> columnNames) throws SolrServerException, IOException {
        List<SolrInputDocument> docs = new ArrayList<>();
        SolrInputDocument doc;
        int count = 1;
        for (int i = 0; i < result.getRowCount(); i++) {
            String file_id = result.getString(i, "file_id");
            SolrDocument solrDocument = solrClient.getById(file_id);
            if (solrDocument == null) {
                continue;
            }
            count++;
            doc = new SolrInputDocument();
            for (String fieldName : solrDocument.getFieldNames()) {
                doc.addField(fieldName, solrDocument.getFieldValue(fieldName));
            }
            for (String columnName : columnNames) {
                doc.remove(Constant.SOLR_DATA_ASSOCIATION_PREFIX + columnName);
                if (columnName.equalsIgnoreCase(Constant._HYREN_S_DATE)) {
                    doc.addField(Constant.SOLR_DATA_ASSOCIATION_PREFIX + columnName, result.getString(i, columnName).replace("-", ""));
                } else {
                    doc.addField(Constant.SOLR_DATA_ASSOCIATION_PREFIX + columnName, result.getString(i, columnName));
                }
            }
            docs.add(doc);
            if (docs.size() % CommonVariables.SOLR_BULK_SUBMISSIONS_NUM == 0) {
                solrClient.add(docs);
                solrClient.commit();
                docs.clear();
                log.info("已追加 " + count + " 条索引！");
            }
        }
        if (!docs.isEmpty()) {
            solrClient.add(docs);
            solrClient.commit();
            docs.clear();
            log.info("已追加完毕，共  " + count + " 条索引！");
        }
    }
}
