package hyren.serv6.agent.job.biz.utils;

import fd.ng.core.utils.BeanUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.DataExtractType;
import hyren.serv6.commons.collection.bean.JDBCBean;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.TbColTarTypeMapBean;
import hyren.serv6.commons.utils.constant.Constant;
import org.apache.commons.lang3.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CollectTableBeanUtil {

    public static List<DataExtractionDef> getTransSeparatorExtractionList(List<DataExtractionDef> data_extraction_def_list) {
        List<DataExtractionDef> data_extraction_defs = new ArrayList<>();
        for (DataExtractionDef data_extraction_def : data_extraction_def_list) {
            DataExtractionDef def = new DataExtractionDef();
            BeanUtil.copyProperties(data_extraction_def, def);
            if (!StringUtil.isEmpty(data_extraction_def.getRow_separator())) {
                String row_sp = StringUtil.unicode2String(data_extraction_def.getRow_separator());
                switch(row_sp) {
                    case "\\r\\n":
                        def.setRow_separator("\r\n");
                        break;
                    case "\\r":
                        def.setRow_separator("\r");
                        break;
                    case "\\n":
                        def.setRow_separator("\n");
                        break;
                }
            }
            if (!StringUtil.isEmpty(data_extraction_def.getDatabase_separatorr())) {
                String separatorr = StringUtil.unicode2String(data_extraction_def.getDatabase_separatorr());
                if (StringUtils.isNumeric(separatorr)) {
                    int parseInt = Integer.parseInt(separatorr);
                    separatorr = Character.toString((char) parseInt);
                }
                def.setDatabase_separatorr(separatorr);
            }
            data_extraction_defs.add(def);
        }
        return data_extraction_defs;
    }

    public static DataExtractionDef getSourceData_extraction_def(List<DataExtractionDef> data_extraction_def_list) {
        List<DataExtractionDef> def_list = getTransSeparatorExtractionList(data_extraction_def_list);
        for (DataExtractionDef data_extraction_def : def_list) {
            if (DataExtractType.YuanShuJuGeShi.getCode().equals(data_extraction_def.getData_extract_type())) {
                return data_extraction_def;
            }
        }
        throw new AppSystemException("找不到原文件格式定义的对象");
    }

    public static void setColTarType(Map<Long, String> tbColTarMap, Map<Long, List<TbColTarTypeMapBean>> tbColMap, List<String> columnList, AgentType agentType) {
        for (Map.Entry<Long, List<TbColTarTypeMapBean>> entry : tbColMap.entrySet()) {
            List<String> tarTypes = new ArrayList<>();
            for (String col : columnList) {
                if (AgentType.DBWenJian == agentType) {
                    List<String> colList = StringUtil.split(col, Constant.METAINFOSPLIT);
                    col = colList.get(0);
                }
                for (TbColTarTypeMapBean tarTypeMapBean : entry.getValue()) {
                    if (tarTypeMapBean.getColumn_name().equalsIgnoreCase(col)) {
                        tarTypes.add(tarTypeMapBean.getColumn_tar_type());
                    }
                }
            }
            tbColTarMap.put(entry.getKey(), StringUtil.join(tarTypes, Constant.METAINFOSPLIT));
        }
    }

    public static JDBCBean setJdbcBean(SourceDataConfBean sdfcBean) {
        JDBCBean jdbcBean = new JDBCBean();
        jdbcBean.setDatabase_drive(sdfcBean.getDatabase_drive());
        jdbcBean.setJdbc_url(sdfcBean.getJdbc_url());
        jdbcBean.setUser_name(sdfcBean.getUser_name());
        jdbcBean.setDatabase_pad(sdfcBean.getDatabase_pad());
        jdbcBean.setDatabase_type(sdfcBean.getDatabase_type());
        jdbcBean.setDatabase_name(sdfcBean.getDatabase_name());
        jdbcBean.setFetch_size(sdfcBean.getFetch_size());
        return jdbcBean;
    }
}
