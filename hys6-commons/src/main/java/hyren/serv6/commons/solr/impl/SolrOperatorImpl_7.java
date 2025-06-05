package hyren.serv6.commons.solr.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.solr.login.SolrLogin;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.solr.utils.QESXMLResponseParser;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import java.io.File;
import java.io.IOException;
import java.util.*;

@Slf4j
@DocClass(desc = "", author = "BY-HLL", createdate = "2022/09/20 0009 上午 10:14")
public class SolrOperatorImpl_7 extends AbsSolrOperatorImpl {

    private CloudSolrClient solrClient$;

    public SolrOperatorImpl_7() {
        connectSolr();
        setSolrClient(solrClient$);
    }

    public SolrOperatorImpl_7(SolrParam solrParam) {
        connectSolr(solrParam, null);
        setSolrClient(solrClient$);
    }

    public SolrOperatorImpl_7(SolrParam solrParam, String configPath) {
        connectSolr(solrParam, configPath);
        setSolrClient(solrClient$);
    }

    public void connectSolr() {
        try {
            solrClient$ = new CloudSolrClient.Builder(Collections.singletonList(CommonVariables.ZK_HOST), Optional.empty()).build();
            solrClient$.setDefaultCollection(CommonVariables.SOLR_COLLECTION);
            solrClient$.setZkClientTimeout(60000);
            solrClient$.setZkConnectTimeout(60000);
            solrClient$.connect();
        } catch (Exception e) {
            log.info("Solr server is connected successfully!");
            log.error(e.getMessage(), e);
        }
    }

    public void connectSolr(SolrParam solrParam, String configPath) {
        if (StringUtil.isBlank(configPath)) {
            setSecConfig(solrParam);
        } else {
            setSecConfig(solrParam, configPath);
        }
        SolrLogin.setZookeeperServerPrincipal(ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        try {
            if (StringUtil.isEmpty(solrParam.getCollection())) {
                throw new AppSystemException("远程连接solr的collection为空！！！");
            }
            if (StringUtil.isEmpty(solrParam.getSolrZkUrl())) {
                throw new AppSystemException("远程连接solr的solr_zk_url为空！！！");
            }
            String collection = solrParam.getCollection();
            log.info("zookeeper address:" + solrParam.getSolrZkUrl());
            log.info("collection's name:" + collection);
            solrClient$ = new CloudSolrClient.Builder(Collections.singletonList(solrParam.getSolrZkUrl()), Optional.empty()).build();
            solrClient$.setDefaultCollection(collection);
            solrClient$.setZkClientTimeout(60000);
            solrClient$.setZkConnectTimeout(60000);
            solrClient$.connect();
            log.info("Solr server is connected successfully!");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Method(desc = "", logicStep = "")
    private void setSecConfig(SolrParam solrParam) {
        String path = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        path = path.replace("\\", "\\\\");
        SolrLogin.setJaasFile(solrParam.getPrinciple_name(), path + StorageTypeKey.keytab_file);
        SolrLogin.setKrb5Config(path + StorageTypeKey.krb5_conf);
    }

    @Method(desc = "", logicStep = "")
    private void setSecConfig(SolrParam solrParam, String path) {
        if (new File(path + StorageTypeKey.keytab_file).exists()) {
            SolrLogin.setJaasFile(solrParam.getPrinciple_name(), path + StorageTypeKey.keytab_file);
        }
        if (new File(path + StorageTypeKey.krb5_conf).exists()) {
            SolrLogin.setKrb5Config(path + StorageTypeKey.krb5_conf);
        }
    }

    @Override
    public void testConnectSolr() {
    }

    @Override
    public List<Map<String, Object>> requestHandler(String... temp) {
        temp = temp.length < 1 ? new String[] { Constant.HANDLER } : temp;
        List<Map<String, Object>> mapArrayList = new ArrayList<>();
        SolrQuery sq = new SolrQuery();
        QueryResponse response;
        ((CloudSolrClient) solrClient).setParser(new QESXMLResponseParser());
        for (String handler : temp) {
            try {
                Map<String, Object> map = new HashMap<>();
                sq.setRequestHandler(handler);
                response = solrClient.query(sq);
                map.put(handler, response.getStatus());
                mapArrayList.add(map);
                log.info("[INFO] Spend time on request to custom handler    " + handler + " : " + response.getQTime() + " ms");
            } catch (SolrServerException | IOException e) {
                log.error("[ERROR} Spend time on request to custom handler:", e);
            }
        }
        return mapArrayList;
    }

    @Method(desc = "", logicStep = "")
    @Override
    public void close() {
        try {
            solrClient.close();
            log.info("solr server is disconnected successfully!");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
