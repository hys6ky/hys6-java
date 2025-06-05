package hyren.serv6.m.contants;

import hyren.serv6.base.codes.CodesItem;
import org.springframework.stereotype.Component;

@Component
public class ItemCodes extends CodesItem {

    static {
        if (!CodesItem.mapCat.containsKey("MetadataSourceEnum"))
            CodesItem.mapCat.put("MetadataSourceEnum", MetadataSourceEnum.class);
        if (!CodesItem.mapCat.containsKey("MetaObjTypeEnum"))
            CodesItem.mapCat.put("MetaObjTypeEnum", MetaObjTypeEnum.class);
    }
}
