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
public class DbmCodeTypeExcelVo {

    @ExcelProperty(value = "", index = 0)
    private String sort_theme;

    @ExcelProperty(value = "", index = 1)
    private String code_encode;

    @ExcelProperty(value = "", index = 2)
    private String code_enname;

    @ExcelProperty(value = "", index = 3)
    private String code_value;

    @ExcelProperty(value = "", index = 4)
    private String code_ensketch;

    @ExcelProperty(value = "", index = 5)
    private String code_endesc;

    @ExcelProperty(value = "", index = 6)
    private String code_cnsketch;

    @ExcelProperty(value = "", index = 7)
    private String code_cndesc;

    @ExcelProperty(value = "", index = 8)
    private String dbm_level;

    @ExcelProperty(value = "", index = 9)
    private String code_remark;

    @ExcelProperty(value = "", index = 10)
    private String status;

    @ExcelProperty(value = "", index = 11)
    private String proposed;
}
