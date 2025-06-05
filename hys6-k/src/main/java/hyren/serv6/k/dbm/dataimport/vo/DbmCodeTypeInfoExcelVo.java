package hyren.serv6.k.dbm.dataimport.vo;

import com.alibaba.excel.annotation.ExcelProperty;
import hyren.serv6.k.utils.easyexcel.ExcelValid;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DbmCodeTypeInfoExcelVo {

    @ExcelValid
    @ExcelProperty(value = "", index = 0)
    protected String code_encode;

    @ExcelValid
    @ExcelProperty(value = "", index = 1)
    protected String code_type_name;

    @ExcelProperty(value = "", index = 2)
    protected String code_remark;

    @ExcelValid
    @ExcelProperty(value = "", index = 3)
    protected String code_value;

    @ExcelValid
    @ExcelProperty(value = "", index = 4)
    protected String code_item_name;

    @ExcelProperty(value = "", index = 5)
    protected String code_desc;
}
