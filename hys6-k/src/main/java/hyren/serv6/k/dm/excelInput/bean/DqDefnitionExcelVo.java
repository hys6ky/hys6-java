package hyren.serv6.k.dm.excelInput.bean;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DqDefnitionExcelVo {

    @ExcelProperty(value = "", index = 0)
    private String reg_name;

    @ExcelProperty(value = "", index = 1)
    private String flags;

    @ExcelProperty(value = "", index = 2)
    private String rule_src;

    @ExcelProperty(value = "", index = 3)
    private String load_strategy;

    @ExcelProperty(value = "", index = 4)
    private String group_seq;

    @ExcelProperty(value = "", index = 5)
    private String rule_tag;

    @ExcelProperty(value = "", index = 6)
    private String case_type;

    @ExcelProperty(value = "", index = 7)
    private String target_tab;

    @ExcelProperty(value = "", index = 8)
    private String target_key_fields;

    @ExcelProperty(value = "", index = 9)
    private String opposite_tab;

    @ExcelProperty(value = "", index = 10)
    private String opposite_key_fields;

    @ExcelProperty(value = "", index = 11)
    private String range_min_val;

    @ExcelProperty(value = "", index = 12)
    private String range_max_val;

    @ExcelProperty(value = "", index = 13)
    private String list_vals;

    @ExcelProperty(value = "", index = 14)
    private String total_corr_fields;

    @ExcelProperty(value = "", index = 15)
    private String total_filter_fields;

    @ExcelProperty(value = "", index = 16)
    private String sub_group_fields;

    @ExcelProperty(value = "", index = 17)
    private String sub_filter_condition;

    @ExcelProperty(value = "", index = 18)
    private String remark;
}
