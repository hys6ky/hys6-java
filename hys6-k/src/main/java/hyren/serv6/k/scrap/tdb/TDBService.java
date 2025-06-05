package hyren.serv6.k.scrap.tdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.base.utils.ActionResult;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.http.HttpClient;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import hyren.serv6.k.scrap.tdb.bean.TdbTableBean;
import hyren.serv6.k.scrap.tdb.bean.TdbTableBeanDto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class TDBService {

    private static final Logger logger = LogManager.getLogger();

    private LayerBean checkStoreLayer(List<TdbTableBean> tdb_table_bean_s) {
        LayerBean sameLayerBean = null;
        Map<String, List<LayerBean>> map = new HashMap<>();
        for (TdbTableBean tdbTableBean : tdb_table_bean_s) {
            List<LayerBean> layerBeans = ProcessingData.getLayerByTable(tdbTableBean.getHyren_name(), Dbo.db());
            for (LayerBean layerBean : layerBeans) {
                if (map.get(layerBean.getDsl_name()) == null) {
                    List<LayerBean> putList = new ArrayList<>();
                    putList.add(layerBean);
                    map.put(layerBean.getDsl_name(), putList);
                } else {
                    map.get(layerBean.getDsl_name()).add(layerBean);
                }
            }
        }
        for (String dsl_name : map.keySet()) {
            List<LayerBean> layerBeans = map.get(dsl_name);
            if (tdb_table_bean_s.size() == layerBeans.size() && (Store_type.DATABASE.getCode().equals(layerBeans.get(0).getStore_type()) || Store_type.HIVE.getCode().equals(layerBeans.get(0).getStore_type()))) {
                sameLayerBean = layerBeans.get(0);
                break;
            }
        }
        if (sameLayerBean == null) {
            throw new BusinessException("你所选的表不在同一数据库类型的存储层中");
        }
        return sameLayerBean;
    }

    private void saveDbmAnalysisConfAndScheduleTab(List<TdbTableBean> tdb_table_bean_s, String sys_class_code) {
        Dbo.execute("DELETE FROM dbm_analysis_conf_tab WHERE sys_class_code = ?", sys_class_code);
        Dbo.execute("DELETE FROM dbm_analysis_schedule_tab WHERE sys_class_code = ?", sys_class_code);
        for (TdbTableBean tdbTableBean : tdb_table_bean_s) {
            String hyren_name = tdbTableBean.getHyren_name();
            Map<String, Object> objectMap = SqlOperator.queryOneObject(Dbo.db(), "select a.collect_type,a.table_name from " + DataStoreReg.TableName + " a join" + " " + DatabaseSet.TableName + " b on a.database_id = b.database_id " + " where a.collect_type in (?,?) and lower(hyren_name) = ? and b.collect_type = ?", AgentType.DBWenJian.getCode(), AgentType.ShuJuKu.getCode(), hyren_name.toLowerCase(), CollectType.TieYuanDengJi.getCode());
            if (objectMap.size() != 0) {
                if (null == objectMap.get("table_name")) {
                    throw new BusinessException("根据登记表名获取实际表名出错!");
                }
                hyren_name = objectMap.get("table_name").toString().toUpperCase();
            }
            logger.error("暂未实现！！");
            throw new SystemBusinessException("暂未实现！");
        }
        Dbo.db().commit();
    }

    private boolean isJoinPk(String dsl_name, String table_name) throws Exception {
        ResultSet resultSet = Dbo.db().queryGetResultSet("select * from dbm_joint_pk_tab where " + "sys_class_code = ? and table_code = ?", dsl_name, table_name);
        return resultSet.next();
    }

    private void analysis_feature(String dsl_name, String table_name, Map<String, String> layerAttr) {
        String url = PropertyParaValue.getString("algorithms_python_serve", "http://127.0.0.1:33333/") + "execute_feature_main";
        HttpClient.ResponseValue resVal = new HttpClient().addData("sys_class_code", dsl_name).addData("table_code", table_name).addData("etl_date", "").addData("date_offset", "").addData("alg", "F5").addData("layerAttr", JsonUtil.toJson(layerAttr)).post(url);
        String bodyString = resVal.getBodyString();
        ActionResult ar = JsonUtil.toObjectSafety(bodyString, ActionResult.class).orElseThrow(() -> new BusinessException("连接" + url + "服务异常"));
        if (!ar.isSuccess()) {
            System.out.println((">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage()));
            throw new BusinessException("Agent通讯异常,请检查Agent是否已启动!!!");
        }
    }

    private void analyse_table_fk(String dsl_name, String table_name, Map<String, String> layerAttr) {
        String url = PropertyParaValue.getString("algorithms_python_serve", "http://127.0.0.1:33333/") + "execute_analyse_table_fk";
        HttpClient.ResponseValue resVal = new HttpClient().addData("sys_class_code", dsl_name).addData("table_code", table_name).addData("start_date", "").addData("date_offset", "").addData("mode", dsl_name).addData("alg", "F5").addData("layerAttr", JsonUtil.toJson(layerAttr)).post(url);
        ActionResult ar = JsonUtil.toObjectSafety(resVal.getBodyString(), ActionResult.class).orElseThrow(() -> new BusinessException("连接" + url + "服务异常"));
        if (!ar.isSuccess()) {
            System.out.println((">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage()));
            throw new BusinessException("Agent通讯异常,请检查Agent是否已启动!!!");
        }
    }

    private void analyse_joint_fk(String dsl_name, Map<String, String> layerAttr) {
        String url = PropertyParaValue.getString("algorithms_python_serve", "http://127.0.0.1:33333/") + "execute_joint_fk_main";
        HttpClient.ResponseValue resVal = new HttpClient().addData("main_table_code", dsl_name).addData("sub_sys_class_code", dsl_name).addData("layerAttr", JsonUtil.toJson(layerAttr)).post(url);
        ActionResult ar = JsonUtil.toObjectSafety(resVal.getBodyString(), ActionResult.class).orElseThrow(() -> new BusinessException("连接" + url + "服务异常"));
        if (!ar.isSuccess()) {
            System.out.println((">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage()));
            throw new BusinessException("Agent通讯异常,请检查Agent是否已启动!!!");
        }
    }

    private void analyse_dim_division(Map<String, String> layerAttr) {
        String url = PropertyParaValue.getString("algorithms_python_serve", "http://127.0.0.1:33333/") + "execute_dim_division_main";
        HttpClient.ResponseValue resVal = new HttpClient().addData("layerAttr", JsonUtil.toJson(layerAttr)).post(url);
        ActionResult ar = JsonUtil.toObjectSafety(resVal.getBodyString(), ActionResult.class).orElseThrow(() -> new BusinessException("连接" + url + "服务异常"));
        if (!ar.isSuccess()) {
            System.out.println((">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage()));
            throw new BusinessException("Agent通讯异常,请检查Agent是否已启动!!!");
        }
    }

    private void analyse_dim_cluster(String dsl_name, Map<String, String> layerAttr) {
        String url = PropertyParaValue.getString("algorithms_python_serve", "http://127.0.0.1:33333/") + "execute_run_dim_cluster_main";
        HttpClient.ResponseValue resVal = new HttpClient().addData("sys_class_code", dsl_name).addData("layerAttr", JsonUtil.toJson(layerAttr)).post(url);
        ActionResult ar = JsonUtil.toObjectSafety(resVal.getBodyString(), ActionResult.class).orElseThrow(() -> new BusinessException("连接" + url + "服务异常"));
        if (!ar.isSuccess()) {
            System.out.println((">>>>>>>>>>>>>>>>>>>>>>>>错误信息为：" + ar.getMessage()));
            throw new BusinessException("Agent通讯异常,请检查Agent是否已启动!!!");
        }
    }

    public List<TdbTableBeanDto> stringToTDBTableBeanDto(String tdbTableBean) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<TdbTableBeanDto> list = null;
        try {
            list = objectMapper.readValue(tdbTableBean, new TypeReference<List<TdbTableBeanDto>>() {
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return list;
    }
}
