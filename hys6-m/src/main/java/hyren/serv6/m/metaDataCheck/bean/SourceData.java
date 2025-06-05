package hyren.serv6.m.metaDataCheck.bean;

import hyren.serv6.m.entity.MetaObjInfo;
import hyren.serv6.m.entity.MetaObjTblCol;
import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class SourceData {

    private List<MetaObjInfo> sourceTables;

    private List<MetaObjInfo> sourceViews;

    private List<MetaObjInfo> sourceMviews;

    private List<MetaObjInfo> sourceProcs;

    private List<MetaObjTblColVo> metaTabeCol;

    private List<MetaObjTblColVo> metaPKTacleCol;

    private Map<String, List<MetaObjTblCol>> sourceTabeCol;

    private Map<String, List<MetaObjTblCol>> sourcrPKTacleCol;
}
