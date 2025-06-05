package hyren.serv6.n.bean;

import lombok.Data;

@Data
public class DataAssetCodingVo {

    private long coding_id;

    private long dir_id;

    private String rule_name_lev1;

    private String rule_code_lev1;

    private int digit_lev1;

    private String start_range_lev1;

    private String end_range_lev1;

    private String rule_name_lev2;

    private String rule_code_lev2;

    private int digit_lev2;

    private String start_range_lev2;

    private String end_range_lev2;

    private String rule_name_lev3;

    private String rule_code_lev3;

    private int digit_lev3;

    private String start_range_lev3;

    private String end_range_lev3;

    private String coding_split;

    private String asset_rule_name;

    private String asset_rule_code;

    private int asset_digit;

    private String start_range_asset;

    private String end_range_asset;
}
