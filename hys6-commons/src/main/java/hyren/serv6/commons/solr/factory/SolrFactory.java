package hyren.serv6.commons.solr.factory;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.utils.constant.CommonVariables;
import java.lang.reflect.Constructor;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/9 0009 上午 10:20")
public class SolrFactory {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static ISolrOperator getSolrOperatorInstance() {
        return getSolrOperatorInstance(CommonVariables.SOLR_IMPL_CLASS_NAME);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "solrParam", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ISolrOperator getSolrOperatorInstance(SolrParam solrParam) {
        return getSolrOperatorInstance(CommonVariables.SOLR_IMPL_CLASS_NAME, solrParam);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "className", desc = "", range = "")
    @Param(name = "solrParam", desc = "", range = "")
    @Param(name = "configPath", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ISolrOperator getSolrOperatorInstance(SolrParam solrParam, String configPath) {
        return getSolrOperatorInstance(CommonVariables.SOLR_IMPL_CLASS_NAME, solrParam, configPath);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "className", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ISolrOperator getSolrOperatorInstance(String className) {
        ISolrOperator solr;
        try {
            solr = (ISolrOperator) Class.forName(className).newInstance();
        } catch (Exception e) {
            throw new AppSystemException("初始化Solr实例实现类失败...！");
        }
        return solr;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "className", desc = "", range = "")
    @Param(name = "solrParam", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ISolrOperator getSolrOperatorInstance(String className, SolrParam solrParam) {
        ISolrOperator solr;
        try {
            Class<?> cl = Class.forName(className);
            Constructor<?> cc = cl.getConstructor(SolrParam.class);
            solr = (ISolrOperator) cc.newInstance(solrParam);
        } catch (Exception e) {
            throw new AppSystemException("初始化Solr实例实现类失败...远程!", e);
        }
        return solr;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "className", desc = "", range = "")
    @Param(name = "solrParam", desc = "", range = "")
    @Param(name = "configPath", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ISolrOperator getSolrOperatorInstance(String ClassName, SolrParam solrParam, String configPath) {
        ISolrOperator solr;
        try {
            Class<?> cl = Class.forName(ClassName);
            Constructor<?> cc = cl.getConstructor(SolrParam.class, String.class);
            solr = (ISolrOperator) cc.newInstance(solrParam, configPath);
        } catch (Exception e) {
            throw new AppSystemException("初始化Solr实例实现类失败...远程! conf: " + configPath, e);
        }
        return solr;
    }
}
