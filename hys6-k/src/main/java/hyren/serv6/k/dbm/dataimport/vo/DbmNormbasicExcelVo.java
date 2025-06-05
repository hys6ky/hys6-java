package hyren.serv6.k.dbm.dataimport.vo;

import com.alibaba.excel.annotation.ExcelProperty;
import hyren.serv6.k.utils.easyexcel.ExcelValid;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.validation.constraints.NotBlank;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DbmNormbasicExcelVo {

    @ExcelProperty(value = "", index = 0)
    @ExcelValid
    protected String sort_name;

    @ExcelProperty(value = "", index = 1)
    @ExcelValid
    protected String norm_ename;

    @ExcelProperty(value = "", index = 2)
    @ExcelValid
    protected String norm_cname;

    @ExcelProperty(value = "", index = 3)
    @ExcelValid
    protected String norm_rename;

    @ExcelProperty(value = "", index = 4)
    protected String norm_aname;

    @ExcelProperty(value = "", index = 5)
    protected String business_def;

    @ExcelProperty(value = "", index = 6)
    protected String business_rule;

    @ExcelProperty(value = "", index = 7)
    protected String dbm_domain;

    @ExcelProperty(value = "", index = 8)
    protected String norm_basis;

    @ExcelProperty(value = "", index = 9)
    protected String data_type;

    @ExcelProperty(value = "", index = 10)
    @ExcelValid
    protected Long col_len;

    @ExcelProperty(value = "", index = 11)
    protected Long decimal_point;

    @ExcelProperty(value = "", index = 12)
    protected String code_type_name;

    @ExcelProperty(value = "", index = 13)
    protected String manage_department;

    @ExcelProperty(value = "", index = 14)
    protected String relevant_department;

    @ExcelProperty(value = "", index = 15)
    protected String origin_system;

    @ExcelProperty(value = "", index = 16)
    protected String formulator;

    @ExcelProperty(value = "", index = 17, converter = NormStatusConverter.class)
    protected String norm_status;
}
