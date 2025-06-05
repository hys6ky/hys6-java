package hyren.serv6.k.dbm.dataimport.vo;

import cn.hutool.core.map.MapUtil;
import com.alibaba.excel.converters.Converter;
import com.alibaba.excel.enums.CellDataTypeEnum;
import com.alibaba.excel.metadata.CellData;
import com.alibaba.excel.metadata.GlobalConfiguration;
import com.alibaba.excel.metadata.property.ExcelContentProperty;
import hyren.serv6.base.codes.DbmDataType;
import hyren.serv6.base.codes.IsFlag;

public class NormStatusConverter implements Converter<String> {

    @Override
    public Class supportJavaTypeKey() {
        return String.class;
    }

    @Override
    public CellDataTypeEnum supportExcelTypeKey() {
        return CellDataTypeEnum.STRING;
    }

    @Override
    public String convertToJavaData(CellData cellData, ExcelContentProperty excelContentProperty, GlobalConfiguration globalConfiguration) throws Exception {
        return "已发布".equals(cellData.getStringValue()) ? IsFlag.Shi.getCode() : IsFlag.Fou.getCode();
    }

    @Override
    public CellData convertToExcelData(String val, ExcelContentProperty excelContentProperty, GlobalConfiguration globalConfiguration) throws Exception {
        return new CellData(IsFlag.Shi.getCode().equals(val) ? "已发布" : "未发布");
    }
}
