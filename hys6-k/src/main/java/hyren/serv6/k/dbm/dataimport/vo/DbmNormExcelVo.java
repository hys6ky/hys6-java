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
public class DbmNormExcelVo {

    @ExcelProperty(value = "", index = 0)
    private String norm_code;

    @ExcelProperty(value = "", index = 1)
    private String sort_theme;

    @ExcelProperty(value = "", index = 2)
    private String sort_class;

    @ExcelProperty(value = "", index = 3)
    private String sort_subClass;

    @ExcelProperty(value = "", index = 4)
    private String norm_cname;

    @ExcelProperty(value = "", index = 5)
    private String norm_ename;

    @ExcelProperty(value = "", index = 6)
    private String norm_aname;

    @ExcelProperty(value = "", index = 7)
    private String business_def;

    @ExcelProperty(value = "", index = 8)
    private String business_rule;

    @ExcelProperty(value = "", index = 9)
    private String dbm_domain;

    @ExcelProperty(value = "", index = 10)
    private String norm_basis;

    @ExcelProperty(value = "", index = 11)
    private String data_type;

    @ExcelProperty(value = "", index = 12)
    private String col_len;

    @ExcelProperty(value = "", index = 13)
    private String decimal_point;

    @ExcelProperty(value = "", index = 14)
    private String code_rule;

    @ExcelProperty(value = "", index = 15)
    private String manage_department;

    @ExcelProperty(value = "", index = 16)
    private String relevant_department;

    @ExcelProperty(value = "", index = 17)
    private String origin_system;

    @ExcelProperty(value = "", index = 18)
    private String related_system;

    @ExcelProperty(value = "", index = 19)
    private String related_systemRel;

    @ExcelProperty(value = "", index = 20)
    private String formulator;
}
