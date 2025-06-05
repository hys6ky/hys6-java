package hyren.serv6.n.bean;

import lombok.Data;

@Data
public class DataAssetSearchDto {

    private long assetId;

    private String mdataId;

    private String assetCName;

    private String assetEName;

    private String assetCode;

    private String assetNormCName;

    private String assetNormEName;

    private String businessRemark;

    private String assetRemark;

    private String businessPk;

    private String businessCName;

    private String techPk;

    private String techCName;

    private String dataAuthCode;

    private String belongDepart;

    private String belongBy;

    private String manageDepart;

    private String manageBy;

    private String assetBy;

    private String assetDate;

    private String assetTime;

    private String assetType;

    private String shareType;

    private String shareMetho;

    private String securityLevel;

    private String amountUnit;
}
