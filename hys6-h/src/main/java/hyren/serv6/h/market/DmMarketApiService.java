package hyren.serv6.h.market;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.util.JdbcConstants;
import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.base.utils.fileutil.FileDownloadUtil;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.commons.utils.xlstoxml.util.ExcelUtil;
import hyren.serv6.h.market.dcolRelationStore.DcolRelationStoreService;
import hyren.serv6.h.market.dmcategory.DmCategoryService;
import hyren.serv6.h.market.dminfo.DmInfoService;
import hyren.serv6.h.market.dmjobtable.DmJobTableInfoService;
import hyren.serv6.h.market.dmmoduletable.DmModuleTableService;
import hyren.serv6.h.market.moduletablefield.DataTableFieldService;
import hyren.serv6.h.market.moduletablefield.DmModuleTableFieldInfoDto;
import hyren.serv6.h.market.util.DmModuleTableUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.multipart.MultipartFile;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import static hyren.serv6.base.user.UserUtil.getUserId;

@Slf4j
@Service
public class DmMarketApiService {

    public static final String TargetColumn = "targecolumn";

    public static final String SourceColumn = "sourcecolumn";

    private static final String FLINK_JOB_SH = "process-flink-job-command.sh";

    @Autowired
    DmInfoService dmInfoService;

    @Autowired
    DmCategoryService dmCategoryService;

    public List<Node> getMarketTreeData() {
        List<Node> nodes = new ArrayList<>();
        List<DmInfo> infoList = dmInfoService.findDmInfosByUserId();
        if (!CollectionUtils.isEmpty(infoList)) {
            for (DmInfo info : infoList) {
                Node node = new Node();
                node.setId(info.getData_mart_id().toString());
                node.setLabel(info.getMart_name());
                node.setDescription(info.getMart_desc());
                node.setData_own_type("Market");
                node.setChildren(getDmCategoryTreeDataByDataMart(info.getData_mart_id()));
                nodes.add(node);
            }
        }
        return nodes;
    }

    private List<Node> getDmCategoryTreeDataByDataMart(long data_mart_id) {
        isDmInfoExist(data_mart_id);
        List<DmCategory> categoryList = dmCategoryService.findDmCategorysByDmInfoId(data_mart_id);
        List<Node> nodes = new ArrayList<>();
        if (!CollectionUtils.isEmpty(categoryList)) {
            List<DmCategory> residue = new ArrayList<>();
            for (DmCategory category : categoryList) {
                if (category.getData_mart_id().equals(category.getParent_category_id())) {
                    Node node = new Node();
                    node.setId(category.getCategory_id().toString());
                    node.setLabel(category.getCategory_name());
                    node.setParent_id(category.getParent_category_id().toString());
                    node.setDescription(category.getCategory_desc());
                    node.setChildren(new ArrayList<>());
                    nodes.add(node);
                } else {
                    residue.add(category);
                }
            }
            for (Node node : nodes) {
                generateTreeNodeByDmCategoryList(node, Long.parseLong(node.getId()), residue);
            }
        }
        return nodes;
    }

    private void isDmInfoExist(long data_mart_id) {
        if (Dbo.queryNumber("select count(*) from " + DmInfo.TableName + " where data_mart_id=?", data_mart_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException(data_mart_id + "对应加工工程已不存在");
        }
    }

    private void generateTreeNodeByDmCategoryList(Node node, Long pId, List<DmCategory> categoryList) {
        if (categoryList.size() <= 0) {
            return;
        }
        List<DmCategory> residue = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();
        for (DmCategory category : categoryList) {
            if (category.getParent_category_id().equals(pId)) {
                Node n = new Node();
                n.setId(category.getCategory_id().toString());
                n.setLabel(category.getCategory_name());
                n.setParent_id(category.getParent_category_id().toString());
                n.setDescription(category.getCategory_desc());
                n.setChildren(new ArrayList<>());
                nodes.add(n);
            } else {
                residue.add(category);
            }
        }
        node.getChildren().addAll(nodes);
        categoryList.clear();
        categoryList.addAll(residue);
        if (!nodes.isEmpty()) {
            for (Node n : nodes) {
                this.generateTreeNodeByDmCategoryList(n, Long.parseLong(n.getId()), categoryList);
            }
        }
    }

    public List<DataStoreLayer> searchDataStore() {
        return Dbo.queryList(DataStoreLayer.class, "SELECT * from " + DataStoreLayer.TableName + " where dsl_goal = ?", IsFlag.Shi.getCode());
    }

    public String searchDataStoreById(long dsl_id) {
        Map<String, Object> storeLayer = Dbo.queryOneObject("select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id);
        List<Map<String, Object>> layerAndAdded = Dbo.queryList("select * from " + DataStoreLayerAdded.TableName + " where dsl_id=?", dsl_id);
        List<Map<String, Object>> layerAndAttr = Dbo.queryList("select * from " + DataStoreLayerAttr.TableName + " where dsl_id=? order by is_file", dsl_id);
        storeLayer.put("layerAndAdded", layerAndAdded);
        storeLayer.put("layerAndAttr", layerAndAttr);
        return AesUtil.encrypt(JsonUtil.toJson(storeLayer));
    }

    public Boolean checkOracle(String dsl_id, String datatable_en_name) {
        DataStoreLayerAttr data_store_layer_attr = new DataStoreLayerAttr();
        data_store_layer_attr.setDsl_id(dsl_id);
        List<DataStoreLayerAttr> data_store_layer_attrs = Dbo.queryList(DataStoreLayerAttr.class, "select * from " + DataStoreLayerAttr.TableName + " where storage_property_key like ? and storage_property_val like ? and dsl_id = ?", "%jdbc_url%", "%oracle%", data_store_layer_attr.getDsl_id());
        if (data_store_layer_attrs.isEmpty()) {
            return true;
        } else {
            return datatable_en_name.length() <= 26;
        }
    }

    public List<Object> getAllFieldType(long dsl_id) {
        DataStoreLayer data_store_layer = Dbo.queryOneObject(DataStoreLayer.class, "SELECT * FROM " + DataStoreLayer.TableName + " WHERE DSL_ID = ? ", dsl_id).orElse(new DataStoreLayer());
        if (Store_type.ofEnumByCode(data_store_layer.getStore_type()) == Store_type.DATABASE) {
            List<DataStoreLayerAttr> data_store_layer_attrs = Dbo.queryList(DataStoreLayerAttr.class, "SELECT * FROM " + DataStoreLayerAttr.TableName + " WHERE DSL_ID = ? ", dsl_id);
            for (DataStoreLayerAttr data_store_layer_attr : data_store_layer_attrs) {
                if (data_store_layer_attr.getStorage_property_key().equals(StorageTypeKey.database_type)) {
                    return Dbo.queryOneColumnList("SELECT DATABASE_TYPE2 FROM " + DatabaseTypeMapping.TableName + " WHERE UPPER(DATABASE_NAME2) " + "= UPPER(?) GROUP BY DATABASE_TYPE2 UNION SELECT DATABASE_TYPE1 FROM " + DatabaseTypeMapping.TableName + " WHERE UPPER(DATABASE_NAME1) = UPPER(?) GROUP BY DATABASE_TYPE1", data_store_layer_attr.getStorage_property_val(), data_store_layer_attr.getStorage_property_val());
                }
            }
        }
        return Dbo.queryOneColumnList("SELECT DATABASE_TYPE2 FROM " + DatabaseTypeMapping.TableName + " WHERE UPPER(DATABASE_NAME2) " + "= UPPER(?) GROUP BY DATABASE_TYPE2 UNION SELECT DATABASE_TYPE1 FROM " + DatabaseTypeMapping.TableName + " WHERE UPPER(DATABASE_NAME1) = UPPER(?) GROUP BY DATABASE_TYPE1", Store_type.ofEnumByCode(data_store_layer.getStore_type()).getValue(), Store_type.ofEnumByCode(data_store_layer.getStore_type()).getValue());
    }

    public List<Object> getAllFieldTypeByTableId(long tab_id) {
        OptionalLong optionalLong = SqlOperator.queryNumber(Dbo.db(), "select dsl_id from " + DtabRelationStore.TableName + " where tab_id = ?", tab_id);
        if (optionalLong.isPresent()) {
            return getAllFieldType(optionalLong.getAsLong());
        }
        return null;
    }

    public List<Map<String, Object>> getColumnDslMore(Long dslId) {
        return Dbo.queryList("select dslad_id,dsla_storelayer from " + DataStoreLayerAdded.TableName + " where dsl_id = ?", dslId);
    }

    @Autowired
    DcolRelationStoreService dcolRelationStoreService;

    @Autowired
    DataTableFieldService dataTableFieldService;

    public boolean addDmDataTableFields(List<DmModuleTableFieldInfoDto> datatableFieldInfos, String datatable_id, List<DcolRelationStore> dcolRelationStores, long dsl_id) {
        long module_table_id = Long.parseLong(datatable_id);
        if (datatableFieldInfos.isEmpty()) {
            Dbo.execute(" delete from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", module_table_id);
            return true;
        }
        for (DmModuleTableFieldInfo datatableFieldInfo : datatableFieldInfos) {
            if (StringUtil.isBlank(datatableFieldInfo.getField_en_name()) || StringUtil.isBlank(datatableFieldInfo.getField_type())) {
                throw new BusinessException("please check fields  is not null...");
            }
        }
        Set<String> set = new HashSet<>();
        for (DmModuleTableFieldInfo datatableFieldInfo : datatableFieldInfos) {
            set.add(datatableFieldInfo.getField_en_name());
        }
        if (set.size() != datatableFieldInfos.size()) {
            throw new BusinessException("fields is repeat, please change ...");
        }
        List<DmModuleTableFieldInfoDto> datatableFieldInfoList = dataTableFieldService.saveDatatableFieldInfos(datatable_id, datatableFieldInfos);
        dcolRelationStoreService.saveDcolRelationStore(datatableFieldInfoList, dcolRelationStores);
        List<DataStoreLayerAdded> layerAddeds = Dbo.queryList(DataStoreLayerAdded.class, "select * from " + DataStoreLayerAdded.TableName + " where dsl_id = ?", dsl_id);
        Map<Long, List<DataStoreLayerAdded>> layerAddedMap = new HashMap<>();
        if (!layerAddeds.isEmpty()) {
            layerAddedMap = layerAddeds.stream().collect(Collectors.groupingBy(DataStoreLayerAdded::getDslad_id));
        }
        List<String> partitionFields = new ArrayList<>();
        for (DcolRelationStore dcolRelationStore : dcolRelationStores) {
            Long dsladId = dcolRelationStore.getDslad_id();
            if (!layerAddedMap.isEmpty()) {
                List<DataStoreLayerAdded> storeLayerAddeds = layerAddedMap.get(dsladId);
                for (DataStoreLayerAdded layerAdded : storeLayerAddeds) {
                    if (StoreLayerAdded.FenQuLie == StoreLayerAdded.ofEnumByCode(layerAdded.getDsla_storelayer())) {
                        List<DmModuleTableFieldInfo> collect = datatableFieldInfos.stream().filter(s -> s.getCsi_number().equals(dcolRelationStore.getCsi_number().toString())).collect(Collectors.toList());
                        String field_en_name = collect.get(0).getField_en_name();
                        if (!partitionFields.contains(field_en_name)) {
                            partitionFields.add(field_en_name);
                        }
                    }
                }
            }
        }
        if (!datatableFieldInfos.isEmpty()) {
            DmModuleTable moduleTable = Dbo.queryOneObject(DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where module_table_id = ?", module_table_id).orElseThrow(() -> new BusinessException("查询模型表信息失败"));
            List<DmModuleTableFieldInfo> dmModuleTableFieldInfos = new ArrayList<>();
            for (DmModuleTableFieldInfoDto datatableFieldInfo : datatableFieldInfos) {
                DmModuleTableFieldInfo dest = new DmModuleTableFieldInfo();
                BeanUtil.copyProperties(datatableFieldInfo, dest);
                dmModuleTableFieldInfos.add(dest);
            }
            DmModuleTableUtil.addHyrenFieldsByModuleFields(dmModuleTableFieldInfos, moduleTable);
            DmModuleTableUtil.createModuleTableIfNotExistByModuleFields(dsl_id, dmModuleTableFieldInfos, partitionFields, moduleTable.getModule_table_en_name());
        }
        return true;
    }

    public List<Node> getTreeDataInfo() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.FALSE);
        treeConf.setShowDCLRealtime(Boolean.TRUE);
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(TreePageSource.MARKET, UserUtil.getUser(), treeConf);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }

    public List<Map<String, Object>> getColumnMore(Long datatable_id) {
        return Dbo.queryList("select dslad_id,dsla_storelayer from " + DataStoreLayerAdded.TableName + " t1 " + "left join " + DtabRelationStore.TableName + " t2 on t1.dsl_id = t2.dsl_id " + "where t2.tab_id = ? and t2.data_source = ? order by dsla_storelayer", datatable_id, StoreLayerDataSource.DM.getCode());
    }

    public List<Map<String, Object>> getColumnFromDatabase(Long datatable_id, String isTemp, Long jobTableId) {
        if (StringUtil.isBlank(isTemp) || isTemp.equals(IsFlag.Fou.getCode())) {
            List<Map<String, Object>> field_list = Dbo.queryList("select * from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ? order by field_seq", datatable_id);
            DmModuleTableFieldInfo dmModuleTableFieldInfo = new DmModuleTableFieldInfo();
            for (Map<String, Object> map : field_list) {
                long module_field_id = Long.parseLong(map.get("module_field_id").toString());
                dmModuleTableFieldInfo.setModule_field_id(module_field_id);
                List<Map<String, Object>> list2 = Dbo.queryList("select dsla_storelayer from " + DataStoreLayerAdded.TableName + " t1 left join " + DcolRelationStore.TableName + " t2 on t1.dslad_id = t2.dslad_id where t2.col_id = ? and t2.data_source = ?", dmModuleTableFieldInfo.getModule_field_id(), StoreLayerDataSource.DM.getCode());
                if (list2 != null) {
                    for (Map<String, Object> everymap : list2) {
                        String dsla_storelayer = everymap.get("dsla_storelayer").toString();
                        map.put(StoreLayerAdded.ofValueByCode(dsla_storelayer), true);
                    }
                }
                if (jobTableId != null) {
                    Map<String, Object> dbProcess = Dbo.queryOneObject(" select jobtab_field_process,jobtab_process_mapping from " + DmJobTableFieldInfo.TableName + " where module_field_id = ? and jobtab_id = ?", module_field_id, jobTableId);
                    if (!dbProcess.isEmpty()) {
                        map.put("field_process", dbProcess.get("jobtab_field_process"));
                        map.put("process_mapping", dbProcess.get("jobtab_process_mapping"));
                    }
                }
            }
            return field_list;
        } else {
            List<Map<String, Object>> run_list = Dbo.queryList("select jobtab_field_id,module_field_id, jobtab_field_en_name AS field_en_name, jobtab_field_cn_name AS field_cn_name, " + " jobtab_field_seq AS field_seq,jobtab_field_type AS field_type, " + " jobtab_field_length AS field_length,jobtab_field_process AS field_process, " + " jobtab_process_mapping AS process_mapping,jobtab_group_mapping AS group_mapping  from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ? order by jobtab_field_seq", jobTableId);
            List<Map<String, Object>> not_run_list = Dbo.queryList("select jobtab_field_id,module_field_id, jobtab_field_en_name AS field_en_name, jobtab_field_cn_name AS field_cn_name, " + " jobtab_field_seq AS field_seq,jobtab_field_type AS field_type, " + " jobtab_field_length AS field_length,jobtab_field_process AS field_process, " + " jobtab_process_mapping AS process_mapping,jobtab_group_mapping AS group_mapping  from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ? order by jobtab_field_seq", jobTableId);
            List<Map<String, Object>> field_list;
            if (!not_run_list.isEmpty()) {
                field_list = not_run_list;
            } else {
                field_list = run_list;
            }
            return field_list;
        }
    }

    public List<Map<String, Object>> getFromColumnList(Long datatable_id) {
        Optional<DmJobTableInfo> doi_run = Dbo.queryOneObject(DmJobTableInfo.class, "select jobtab_view_sql from " + DmJobTableInfo.TableName + " where module_table_id = ? ", datatable_id);
        DmJobTableInfo dmJobTableInfo;
        if (doi_run.isPresent()) {
            dmJobTableInfo = doi_run.get();
        } else {
            return null;
        }
        String view_sql = dmJobTableInfo.getJobtab_view_sql();
        view_sql = StringUtil.replace(view_sql, "  ", Constant.SPACE);
        DruidParseQuerySql druidParseQuerySql = new DruidParseQuerySql(view_sql);
        List<String> columnNameList = druidParseQuerySql.parseSelectAliasField();
        List<Map<String, Object>> columnlist = new ArrayList<>();
        for (int i = 0; i < columnNameList.size(); i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("value", columnNameList.get(i));
            map.put("code", i);
            columnlist.add(map);
        }
        return columnlist;
    }

    public Boolean getIfHbase(Long datatable_id) {
        OptionalLong optionalLong = Dbo.queryNumber("select count(*) from " + DataStoreLayer.TableName + " t1 left join " + DtabRelationStore.TableName + " t2 " + "on t1.dsl_id = t2.dsl_id where t2.tab_id = ? and t1.store_type = ? and t2.data_source = ?", datatable_id, Store_type.HBASE.getCode(), StoreLayerDataSource.DM.getCode());
        return optionalLong.isPresent() && optionalLong.getAsLong() > 0;
    }

    public List<DmModuleTable> getTableIdFromSameNameTableId(Long datatable_id) {
        return Dbo.queryList(DmModuleTable.class, "select module_table_id  from " + DmModuleTable.TableName + " where module_table_en_name in (select module_table_en_name from " + DmModuleTable.TableName + " where module_table_id = ?) and module_table_id != ? order by module_table_c_date,module_table_c_time", datatable_id, datatable_id);
    }

    public String getQuerySql(Long jobtab_id) {
        DmJobTableInfo dmJobTableInfo = Dbo.queryOneObject(DmJobTableInfo.class, "SELECT * FROM " + DmJobTableInfo.TableName + " WHERE jobtab_id = ?", jobtab_id).orElseThrow(() -> new BusinessException("sql failed."));
        return AesUtil.encrypt(dmJobTableInfo.getJobtab_execute_sql());
    }

    public Map<String, Object> queryAllColumnOnTableName(String source, String id) {
        Validator.notBlank(source, "查询数据层为空!");
        Validator.notBlank(id, "查询数据表id为空!");
        return DataTableUtil.getTableInfoAndColumnInfo(source, id);
    }

    public List<String> getAllFromSqlColumns(String querySql) {
        try {
            querySql = StringUtil.replace(querySql, "  ", Constant.SPACE);
            DruidParseQuerySql druidParseQuerySql = new DruidParseQuerySql(querySql);
            return druidParseQuerySql.parseSelectAliasField();
        } catch (Exception e) {
            if (!StringUtil.isEmpty(e.getMessage())) {
                log.error(e.getMessage());
                throw new BusinessException(e.getMessage());
            }
        }
        return new ArrayList<>();
    }

    public Map<String, Object> getJobDataInfoById(long task_id, long datatable_id) {
        return Dbo.queryOneObject("SELECT T1.TASK_ID,T1.TASK_NUMBER,T1.TASK_NAME,T2.module_table_en_name," + "T2.module_table_cn_name,T2.module_table_id" + " FROM " + DmTaskInfo.TableName + " T1 JOIN " + DmModuleTable.TableName + " T2 ON T2.module_table_id = T1.module_table_id" + " WHERE T1.TASK_ID = ? AND T2.module_table_id = ?", task_id, datatable_id);
    }

    public boolean addTaskDataTableAndField(Map<String, Object> req) {
        try {
            Long jobTableId = PrimayKeyGener.getNextId();
            if (!Objects.isNull(req.get("jobtab_id")) && !StringUtil.isBlank(req.get("jobtab_id").toString())) {
                jobTableId = ReqDataUtils.getLongData(req, "jobtab_id");
            }
            long taskId = ReqDataUtils.getLongData(req, "task_id");
            Long module_table_id = Objects.isNull(req.get("module_table_id")) ? null : Long.parseLong(req.get("module_table_id").toString());
            String datatable_en_name = ReqDataUtils.getStringData(req, "module_table_en_name");
            String datatable_cn_name = ReqDataUtils.getStringData(req, "module_table_cn_name");
            String is_temp = ReqDataUtils.getStringData(req, "is_temp");
            long step_number = ReqDataUtils.getLongData(req, "step_number");
            List<Map<String, Object>> columnList = JsonUtil.toObject(req.get("module_table_field_info").toString(), new TypeReference<List<Map<String, Object>>>() {
            });
            List<Map<String, Object>> colStorageList = JsonUtil.toObject(req.get("dm_column_storage").toString(), new TypeReference<List<Map<String, Object>>>() {
            });
            List<DmModuleTableFieldInfo> dmModuleTableFieldInfos = Dbo.queryList(DmModuleTableFieldInfo.class, "select * from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", module_table_id);
            DtabRelationStore relationStore = Dbo.queryOneObject(DtabRelationStore.class, "select * from " + DtabRelationStore.TableName + " where tab_id = ?", module_table_id).orElseThrow(() -> new BusinessException("查询数据表存储关系错误"));
            Long dslId = relationStore.getDsl_id();
            List<DataStoreLayerAdded> layerAddeds = Dbo.queryList(DataStoreLayerAdded.class, "select * from " + DataStoreLayerAdded.TableName + " where dsl_id = ?", dslId);
            Map<Long, List<DataStoreLayerAdded>> layerAddedMap = new HashMap<>();
            if (!layerAddeds.isEmpty()) {
                layerAddedMap = layerAddeds.stream().collect(Collectors.groupingBy(DataStoreLayerAdded::getDslad_id));
            }
            List<String> partitionFields = new ArrayList<>();
            List<DmJobTableFieldInfo> dmJobTableFieldInfos = new ArrayList<>();
            for (Map<String, Object> map : columnList) {
                DmJobTableFieldInfo dmJobTableFieldInfo = new DmJobTableFieldInfo();
                dmJobTableFieldInfo.setJobtab_id(jobTableId);
                long moduleFieldId = Objects.isNull(map.get("module_field_id")) ? PrimayKeyGener.getNextId() : Long.parseLong(map.get("module_field_id").toString());
                String field_en_name = ReqDataUtils.getStringData(map, "field_en_name");
                String field_cn_name = ReqDataUtils.getStringData(map, "field_cn_name");
                String field_seq = ReqDataUtils.getStringData(map, "field_seq");
                String field_type = ReqDataUtils.getStringData(map, "field_type");
                String field_length = ReqDataUtils.getStringData(map, "field_length");
                dmJobTableFieldInfo.setModule_field_id(moduleFieldId);
                dmJobTableFieldInfo.setJobtab_field_id(PrimayKeyGener.getNextId());
                dmJobTableFieldInfo.setJobtab_field_en_name(field_en_name);
                dmJobTableFieldInfo.setJobtab_field_cn_name(field_cn_name);
                dmJobTableFieldInfo.setJobtab_field_seq(field_seq);
                dmJobTableFieldInfo.setJobtab_field_type(field_type);
                if (field_length.equals("0")) {
                    dmJobTableFieldInfo.setJobtab_field_length(null);
                } else {
                    dmJobTableFieldInfo.setJobtab_field_length(field_length);
                }
                dmJobTableFieldInfo.setJobtab_field_process(ReqDataUtils.getStringData(map, "field_process"));
                dmJobTableFieldInfo.setJobtab_group_mapping(ReqDataUtils.getStringData(map, "group_mapping"));
                dmJobTableFieldInfo.setJobtab_process_mapping(ReqDataUtils.getStringData(map, "process_mapping"));
                dmJobTableFieldInfo.setJobtab_field_desc(ReqDataUtils.getStringData(map, "remark"));
                dmJobTableFieldInfos.add(dmJobTableFieldInfo);
                if (is_temp.equals(IsFlag.Fou.getCode())) {
                    if (dmModuleTableFieldInfos.isEmpty()) {
                        DmModuleTableFieldInfo dmModuleTableFieldInfo = new DmModuleTableFieldInfo();
                        dmModuleTableFieldInfo.setModule_field_id(moduleFieldId);
                        dmModuleTableFieldInfo.setField_en_name(field_en_name);
                        dmModuleTableFieldInfo.setField_cn_name(field_cn_name);
                        dmModuleTableFieldInfo.setField_seq(field_seq);
                        dmModuleTableFieldInfo.setField_type(field_type);
                        dmModuleTableFieldInfo.setField_length(field_length);
                        dmModuleTableFieldInfo.setModule_table_id(module_table_id);
                        dmModuleTableFieldInfo.add(Dbo.db());
                        Dbo.execute(" delete from " + DcolRelationStore.TableName + " where col_id = ?", moduleFieldId);
                        for (Map<String, Object> dcolMap : colStorageList) {
                            String csi_number = dcolMap.get("csi_number").toString();
                            if (map.get("csi_number").toString().equals(csi_number)) {
                                DcolRelationStore dcolRelationStore = new DcolRelationStore();
                                dcolRelationStore.setDslad_id(dcolMap.get("dslad_id").toString());
                                if (!layerAddedMap.isEmpty()) {
                                    List<DataStoreLayerAdded> dataStoreLayerAddeds = layerAddedMap.get(dcolRelationStore.getDslad_id());
                                    for (DataStoreLayerAdded layerAdded : dataStoreLayerAddeds) {
                                        if (StoreLayerAdded.FenQuLie == StoreLayerAdded.ofEnumByCode(layerAdded.getDsla_storelayer())) {
                                            if (!partitionFields.contains(field_en_name)) {
                                                partitionFields.add(field_en_name);
                                            }
                                        }
                                    }
                                }
                                dcolRelationStore.setCsi_number(csi_number);
                                dcolRelationStore.setData_source(StoreLayerDataSource.DM.getCode());
                                dcolRelationStore.setCol_id(moduleFieldId);
                                dcolRelationStore.add(Dbo.db());
                            }
                        }
                    }
                }
            }
            String view_sql = ReqDataUtils.getStringData(req, "job_table_view_sql");
            String executeSql = getExecuteSql(dmJobTableFieldInfos, view_sql);
            DmJobTableInfo dmJobTableInfo = new DmJobTableInfo();
            dmJobTableInfo.setJobtab_id(jobTableId);
            dmJobTableInfo.setTask_id(taskId);
            dmJobTableInfo.setJobtab_step_number(String.valueOf(step_number));
            dmJobTableInfo.setJobtab_view_sql(view_sql);
            dmJobTableInfo.setJobtab_execute_sql(executeSql);
            dmJobTableInfo.setModule_table_id(module_table_id);
            dmJobTableInfo.setJobtab_en_name(datatable_en_name);
            dmJobTableInfo.setJobtab_cn_name(datatable_cn_name);
            if (is_temp.equals(IsFlag.Shi.getCode())) {
                dmJobTableInfo.setJobtab_is_temp(IsFlag.Shi.getCode());
            } else {
                dmJobTableInfo.setJobtab_is_temp(IsFlag.Fou.getCode());
            }
            delJob(String.valueOf(jobTableId));
            for (DmJobTableFieldInfo dmJobTableFieldInfo : dmJobTableFieldInfos) {
                dmJobTableFieldInfo.add(Dbo.db());
            }
            dmJobTableInfo.add(Dbo.db());
            saveBloodRelationToPGTable(executeSql, jobTableId);
            if (IsFlag.Fou == IsFlag.ofEnumByCode(is_temp)) {
                DmModuleTable moduleTable = Dbo.queryOneObject(DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where module_table_id = ?", module_table_id).orElseThrow(() -> new BusinessException(String.format("根据模型表ID 【%s】查询模型表信息失败", module_table_id)));
                DmModuleTableUtil.addHyrenFields(dmJobTableFieldInfos, moduleTable);
                String module_table_en_name = ReqDataUtils.getStringData(req, "module_table_en_name");
                DmModuleTableUtil.createModuleTableIfNotExist(dslId, dmJobTableFieldInfos, partitionFields, module_table_en_name);
            }
            return true;
        } catch (NumberFormatException e) {
            log.error("add failed..  please check ..., %s", e);
            throw new BusinessException("add failed..  please check ...");
        }
    }

    private boolean checkJobVersion(long jobtab_id, List<DmJobTableFieldInfo> dmJobTableFieldInfos) {
        boolean result = false;
        DmJobTableInfo dmJobTableInfo = dmJobTableInfoService.findDmTaskDataTableByTaskId(String.valueOf(jobtab_id));
        if (result) {
            delJob(String.valueOf(jobtab_id));
            return true;
        }
        List<DmJobTableFieldInfo> dbFields = dmJobTableInfoService.findFieldsByJobTabId(String.valueOf(jobtab_id));
        if (dbFields.size() != dmJobTableFieldInfos.size()) {
            return true;
        }
        for (DmJobTableFieldInfo dmJobTableFieldInfo : dmJobTableFieldInfos) {
            List<DmJobTableFieldInfo> collect = dbFields.stream().filter(fieldInfo -> fieldInfo.getJobtab_field_en_name().equals(dmJobTableFieldInfo.getJobtab_field_en_name()) && fieldInfo.getJobtab_field_cn_name().equals(dmJobTableFieldInfo.getJobtab_field_cn_name()) && fieldInfo.getJobtab_field_type().equals(dmJobTableFieldInfo.getJobtab_field_type()) && fieldInfo.getJobtab_field_length().equals(dmJobTableFieldInfo.getJobtab_field_length()) && fieldInfo.getJobtab_field_process().equals(dmJobTableFieldInfo.getJobtab_field_process()) && fieldInfo.getJobtab_process_mapping().equals(dmJobTableFieldInfo.getJobtab_process_mapping()) && fieldInfo.getJobtab_group_mapping().equals(dmJobTableFieldInfo.getJobtab_group_mapping())).collect(Collectors.toList());
            if (collect.size() != 1) {
                return true;
            }
        }
        return false;
    }

    public void saveBloodRelationToPGTable(String querySql, Long jobTableId) {
        delBlood(jobTableId.toString());
        DruidParseQuerySql dpqs = new DruidParseQuerySql();
        HashMap<String, Object> bloodRelationMap = dpqs.getBloodRelationMap(querySql);
        Iterator<Map.Entry<String, Object>> iterator = bloodRelationMap.entrySet().iterator();
        Map<String, Object> tableMap = new HashMap<>();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String columnname = entry.getKey();
            @SuppressWarnings("unchecked")
            ArrayList<HashMap<String, Object>> list = (ArrayList<HashMap<String, Object>>) entry.getValue();
            for (HashMap<String, Object> map : list) {
                String sourcecolumn = map.get(DruidParseQuerySql.sourcecolumn).toString().toLowerCase();
                String sourcetable = map.get(DruidParseQuerySql.sourcetable).toString().toLowerCase();
                List<Map<String, Object>> templist = new ArrayList<>();
                if (!tableMap.containsKey(sourcetable)) {
                    tableMap.put(sourcetable, templist);
                } else {
                    templist = (ArrayList<Map<String, Object>>) tableMap.get(sourcetable);
                }
                Map<String, Object> tempmap = new HashMap<>();
                tempmap.put(TargetColumn, columnname.toLowerCase());
                tempmap.put(SourceColumn, sourcecolumn.toLowerCase());
                templist.add(tempmap);
                tableMap.put(sourcetable, templist);
            }
        }
        iterator = tableMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String tablename = entry.getKey();
            String dataSourceType;
            try {
                List<LayerBean> layerByTable = ProcessingData.getLayerByTable(tablename, Dbo.db());
                if (layerByTable.isEmpty()) {
                    dataSourceType = DataSourceType.UDL.getCode();
                } else {
                    dataSourceType = layerByTable.get(0).getDst();
                }
            } catch (Exception e) {
                log.info(e.getMessage());
                dataSourceType = DataSourceType.UDL.getCode();
            }
            DmDatatableSource dm_datatable_source = new DmDatatableSource();
            long own_source_table_id = PrimayKeyGener.getNextId();
            dm_datatable_source.setOwn_source_table_id(own_source_table_id);
            dm_datatable_source.setJobtab_id(jobTableId);
            dm_datatable_source.setOwn_source_table_name(tablename);
            dm_datatable_source.setSource_type(dataSourceType);
            dm_datatable_source.add(Dbo.db());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> tempList = (ArrayList<Map<String, Object>>) tableMap.get(tablename.toLowerCase());
            for (Map<String, Object> map : tempList) {
                String targetColumn = map.get(TargetColumn).toString();
                String sourceColumn = map.get(SourceColumn).toString();
                DmMapInfo dmMapInfo = new DmMapInfo();
                dmMapInfo.setMap_id(PrimayKeyGener.getNextId());
                dmMapInfo.setJobtab_id(jobTableId);
                dmMapInfo.setOwn_source_table_id(own_source_table_id);
                dmMapInfo.setSrc_fields_name(sourceColumn);
                dmMapInfo.setTar_field_name(targetColumn);
                dmMapInfo.add(Dbo.db());
                DmOwnSourceField own_source_field = new DmOwnSourceField();
                own_source_field.setOwn_source_table_id(own_source_table_id);
                own_source_field.setOwn_field_id(PrimayKeyGener.getNextId());
                own_source_field.setField_name(sourceColumn);
                String target_type = getDefaultFieldType(Store_type.DATABASE.getCode());
                Map<String, String> fieldType = getFieldType(tablename, sourceColumn, target_type, "");
                String sourceType = fieldType.get("sourcetype");
                own_source_field.setField_type(sourceType);
                own_source_field.add(Dbo.db());
            }
        }
    }

    public static Map<String, String> getFieldType(String sourcetable, String sourcecolumn, String field_type, String dsl_id) {
        Map<String, String> resultmap = new HashMap<>();
        try {
            List<LayerBean> layerByTable = ProcessingData.getLayerByTable(sourcetable, Dbo.db());
            String dataSourceType = layerByTable.get(0).getDst();
            if (dataSourceType.equals(DataSourceType.DCL.getCode())) {
                List<Map<String, Object>> maps = Dbo.queryList("select t2.column_type,t4.dsl_id from " + TableStorageInfo.TableName + " t1 left join " + TableColumn.TableName + " t2 on t1.table_id = t2.table_id" + " left join " + TableStorageInfo.TableName + " t3 on t1.table_id = t3.table_id left join " + DtabRelationStore.TableName + " t4 on t4.tab_id = t3.storage_id " + "where lower(t2.column_name) = ? and lower(t1.hyren_name) = ? ", sourcecolumn.toLowerCase(), sourcetable.toLowerCase());
                if (maps.isEmpty()) {
                    resultmap.put("sourcetype", field_type);
                    resultmap.put("targettype", field_type);
                } else {
                    String column_type = maps.get(0).get("column_type").toString();
                    String DCLdsl_id = maps.get(0).get("dsl_id").toString();
                    resultmap.put("sourcetype", column_type.toLowerCase());
                    if (column_type.contains("(") && column_type.contains(")") && column_type.indexOf("(") < column_type.indexOf(")")) {
                        String field_length = column_type.substring(column_type.indexOf("(") + 1, column_type.indexOf(")"));
                        resultmap.put("field_length", field_length);
                    }
                    column_type = transFormColumnType(column_type, DCLdsl_id);
                    column_type = transFormColumnType(column_type, dsl_id);
                    resultmap.put("targettype", column_type.toLowerCase());
                }
                return resultmap;
            } else if (dataSourceType.equals(DataSourceType.DML.getCode())) {
                List<Map<String, Object>> maps = Dbo.queryList("select field_length,field_type from " + DmModuleTableFieldInfo.TableName + " t1 left join " + DmModuleTable.TableName + " t2 on t1.module_table_id = t2.module_table_id where lower(t2.module_table_en_name) = ? and lower(t1.field_en_name) = ?", sourcetable.toLowerCase(), sourcecolumn.toLowerCase());
                if (maps.isEmpty()) {
                    resultmap.put("sourcetype", field_type);
                    resultmap.put("targettype", field_type);
                } else {
                    String DMLfield_type = maps.get(0).get("field_type").toString();
                    Object DMLfield_length = maps.get(0).get("field_length");
                    if (null != DMLfield_length) {
                        resultmap.put("field_length", DMLfield_length.toString());
                        DMLfield_type = DMLfield_type + "(" + DMLfield_length + ")";
                    }
                    resultmap.put("sourcetype", DMLfield_type.toLowerCase());
                    DMLfield_type = transFormColumnType(DMLfield_type, dsl_id);
                    resultmap.put("targettype", DMLfield_type.toLowerCase());
                }
                return resultmap;
            } else {
                resultmap.put("sourcetype", field_type);
                resultmap.put("targettype", field_type);
                return resultmap;
            }
        } catch (Exception e) {
            resultmap.put("sourcetype", field_type);
            resultmap.put("targettype", field_type);
            return resultmap;
        }
    }

    public static String transFormColumnType(String column_type, String dsl_id) {
        if (StringUtil.isEmpty(dsl_id)) {
            return column_type;
        }
        long dslId = Long.parseLong(dsl_id);
        column_type = column_type.toLowerCase();
        if (column_type.contains("(")) {
            column_type = column_type.substring(0, column_type.indexOf("("));
        }
        List<String> type_contrasts = Dbo.queryOneColumnList("select distinct case" + " when position('(' in t1.target_type) != 0" + " 	then lower(SUBSTR(t1.target_type, 0, position('(' in t1.target_type)))" + " else LOWER(t1.target_type) end as target_type" + " from (" + "	select dsl.DSL_ID, dtm.database_type2 as target_type" + " 	from " + DatabaseTypeMapping.TableName + " dtm" + " 		inner join " + DatabaseInfo.TableName + " di on upper(dtm.database_name2) = upper(di.database_name)" + "		inner join " + DataStoreLayer.TableName + " dsl on upper(di.database_name) = upper(dsl.database_name)" + " 	where upper(substr(dtm.database_type1, 0, position('(' in dtm.database_type1))) = upper(?)" + "	union all" + "	select dsl.DSL_ID, dtm.database_type1 as target_type" + " 	from " + DatabaseTypeMapping.TableName + " dtm" + "		inner join " + DatabaseInfo.TableName + " di on upper(dtm.database_name1) = upper(di.database_name)" + "		inner join " + DataStoreLayer.TableName + " dsl on upper(di.database_name) = upper(dsl.database_name)" + "	where upper(substr(dtm.database_type2, 0, position('(' in dtm.database_type2))) = upper(?)" + " ) as t1" + " where t1.dsl_id = ?", column_type, column_type, dslId);
        log.error(column_type + "\t" + dslId);
        if (type_contrasts.isEmpty()) {
            return column_type;
        } else {
            if (type_contrasts.contains(column_type)) {
                return column_type;
            } else {
                String target_type = type_contrasts.get(0);
                target_type = target_type.toLowerCase();
                if (target_type.contains("(")) {
                    target_type = target_type.substring(0, target_type.indexOf("("));
                }
                return target_type;
            }
        }
    }

    public static String getDefaultFieldType(String storeType) {
        String field_type = "";
        if (storeType.equals(Store_type.DATABASE.getCode())) {
            field_type = "varchar";
        } else if (storeType.equals(Store_type.HIVE.getCode())) {
            field_type = "string";
        } else if (storeType.equals(Store_type.HBASE.getCode())) {
            field_type = "string";
        }
        return field_type;
    }

    public String getExecuteSql(List<DmJobTableFieldInfo> dmJobTableFieldInfos, String querySql) {
        querySql = StringUtil.replace(querySql, "  ", Constant.SPACE);
        DruidParseQuerySql druidParseQuerySql = new DruidParseQuerySql(querySql);
        Map<String, String> selectColumnMap = druidParseQuerySql.getSelectColumnMap();
        boolean flag = true;
        StringBuilder sb = new StringBuilder(1024);
        sb.append("SELECT ");
        for (DmJobTableFieldInfo dmJobTableFieldInfo : dmJobTableFieldInfos) {
            ProcessType processType = ProcessType.ofEnumByCode(dmJobTableFieldInfo.getJobtab_field_process());
            if (ProcessType.DingZhi == processType) {
                sb.append(dmJobTableFieldInfo.getJobtab_process_mapping()).append(" as ").append(dmJobTableFieldInfo.getJobtab_field_en_name()).append(",");
            } else if (ProcessType.ZiZeng == processType) {
                log.info("自增不拼接字段");
            } else if (ProcessType.YingShe == processType) {
                if (StringUtil.isEmpty(dmJobTableFieldInfo.getJobtab_process_mapping())) {
                    sb.append(selectColumnMap.get(dmJobTableFieldInfo.getJobtab_field_en_name().toUpperCase())).append(" as ").append(dmJobTableFieldInfo.getJobtab_field_en_name()).append(",");
                } else {
                    sb.append(selectColumnMap.get(dmJobTableFieldInfo.getJobtab_process_mapping().toUpperCase())).append(" as ").append(dmJobTableFieldInfo.getJobtab_field_en_name()).append(",");
                }
            } else if (ProcessType.HanShuYingShe == processType) {
                sb.append(dmJobTableFieldInfo.getJobtab_process_mapping()).append(" as ").append(dmJobTableFieldInfo.getJobtab_field_en_name()).append(",");
            } else if (ProcessType.FenZhuYingShe == processType && flag) {
                sb.append("#{HyrenFenZhuYingShe}").append(",");
                flag = false;
            } else if (!flag) {
                log.info("第二次进来分组映射不做任何操作，跳过");
            } else {
                throw new BusinessException("错误的字段映射规则");
            }
        }
        String selectSql = sb.toString();
        sb.delete(0, sb.length());
        StringBuilder groupSql = new StringBuilder();
        for (DmJobTableFieldInfo dmJobTableFieldInfo : dmJobTableFieldInfos) {
            if (ProcessType.FenZhuYingShe == ProcessType.ofEnumByCode(dmJobTableFieldInfo.getJobtab_field_process())) {
                String replacement = dmJobTableFieldInfo.getJobtab_process_mapping() + " as " + dmJobTableFieldInfo.getJobtab_field_en_name();
                String[] split = dmJobTableFieldInfo.getJobtab_group_mapping().split("=");
                sb.append(selectSql.replace("#{HyrenFenZhuYingShe}", replacement));
                sb.append("'").append(split[1]).append("'").append(" as ").append(split[0]).append(" FROM ");
                String execute_sql = SQLUtils.format(querySql, JdbcConstants.ORACLE).replace(druidParseQuerySql.getSelectSql(), sb.toString());
                sb.delete(0, sb.length());
                groupSql.append(execute_sql).append(" union all ");
            }
        }
        if (groupSql.length() > 0) {
            return groupSql.delete(groupSql.length() - " union all ".length(), groupSql.length()).toString();
        } else {
            selectSql = selectSql.substring(0, selectSql.length() - 1) + " FROM ";
            return SQLUtils.format(querySql, JdbcConstants.HIVE).replace(druidParseQuerySql.getSelectSql(), selectSql);
        }
    }

    @Autowired
    DmJobTableInfoService dmJobTableInfoService;

    public boolean delJob(String jobtab_id) {
        boolean delJobTableByJobTableId = dmJobTableInfoService.delJobTableByJobTableId(Long.parseLong(jobtab_id));
        boolean delJobFieldByJobTableId = dmJobTableInfoService.delJobFieldByJobTableId(Long.parseLong(jobtab_id));
        boolean delBlood = delBlood(jobtab_id);
        return delJobFieldByJobTableId && delJobTableByJobTableId && delBlood;
    }

    public boolean delBlood(String jobtab_id) {
        try {
            List<Map<String, Object>> mapList = Dbo.queryList(" select own_source_table_id from " + DmDatatableSource.TableName + " where jobtab_id = ?", Long.parseLong(jobtab_id));
            for (Map<String, Object> map : mapList) {
                long own_source_table_id = Long.parseLong(map.get("own_source_table_id").toString());
                Dbo.execute("delete from " + DmOwnSourceField.TableName + " where own_source_table_id = ?", own_source_table_id);
                Dbo.execute("delete from " + DmDatatableSource.TableName + " where own_source_table_id = ?", own_source_table_id);
                Dbo.execute("delete from " + DmMapInfo.TableName + " where own_source_table_id = ?", own_source_table_id);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    public List<EtlSys> getEtlSysData() {
        return Dbo.queryList(EtlSys.class, "SELECT * FROM " + EtlSys.TableName + " WHERE user_id = ?", getUserId());
    }

    public List<EtlSubSysList> getEtlSubSysData(Long etl_sys_id) {
        long countNum = Dbo.queryNumber("SELECT COUNT(1) FROM " + EtlSys.TableName + " WHERE etl_sys_id = ? AND user_id = ?", etl_sys_id, getUserId()).orElseThrow(() -> new BusinessException("SQL查询错误"));
        if (countNum != 1) {
            throw new BusinessException("当前选中工程不存在");
        }
        return Dbo.queryList(EtlSubSysList.class, "SELECT * FROM " + EtlSubSysList.TableName + " WHERE etl_sys_id = ?", etl_sys_id);
    }

    public void generateSingleMarketJob(long etl_sys_id, long sub_sys_id, long module_table_id, String etl_date) {
        Validator.notNull(etl_sys_id, "工程编号不能为空!");
        Validator.notNull(sub_sys_id, "工程子系统不能为空!");
        Validator.notNull(module_table_id, "加工数据表ID不能为空!");
        DmModuleTable dmModuleTable = Dbo.queryOneObject(DmModuleTable.class, "SELECT * FROM " + DmModuleTable.TableName + " WHERE module_table_id = ?", module_table_id).orElseThrow(() -> (new BusinessException("根据加工数据表ID获取加工配置信息的SQL执行失败!")));
        try {
            jobGenerator.setDmModuleTable(dmModuleTable);
            jobGenerator.setEtl_sys_id(etl_sys_id);
            jobGenerator.setSub_sys_id(sub_sys_id);
            jobGenerator.setDb(Dbo.db());
            jobGenerator.setEtl_date(etl_date);
            jobGenerator.generateJob();
        } catch (Exception e) {
            throw new BusinessException("使用加工作业生成器 HyrenProcessJobGenerator 生成作业时发生异常! e: " + e);
        }
    }

    @Autowired
    HyrenProcessJobGenerator jobGenerator;

    public void uploadExcelFile(MultipartFile file, String data_mart_id, String category_id) {
        Workbook workBook;
        try {
            String originalFilename = file.getOriginalFilename();
            File uploadedFile = new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + originalFilename);
            file.transferTo(uploadedFile);
            if (!uploadedFile.exists()) {
                throw new BusinessException("上传文件不存在！");
            }
            workBook = ExcelUtil.getWorkbookFromExcel(uploadedFile);
            saveImportExcel(workBook, data_mart_id, category_id);
            workBook.close();
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException("导入excel文件数据失败！" + e.getMessage());
        }
    }

    @Autowired
    DmModuleTableService dmModuleTableService;

    private void saveImportExcel(Workbook workBook, String data_mart_id, String category_id) {
        int numberOfSheets = workBook.getNumberOfSheets();
        if (numberOfSheets == 0) {
            throw new BusinessException("没有获取到sheet页，请检查文件");
        }
        Sheet sheet = workBook.getSheetAt(0);
        try {
            Row row;
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                row = sheet.getRow(i);
                if (row == null) {
                    break;
                }
                String dsl_name = ExcelUtil.getValue(row.getCell(4)).toString();
                Validator.notBlank(dsl_name, "存储层名称不能为空");
                DataStoreLayer data_store_layer = Dbo.queryOneObject(DataStoreLayer.class, "select * from " + DataStoreLayer.TableName + " where dsl_name = ?", dsl_name.trim()).orElseThrow(() -> new BusinessException("sql查询错误或者当前存储层名称对应存储层信息有误"));
                long dsl_id = data_store_layer.getDsl_id();
                DmModuleTable dmModuleTable = new DmModuleTable();
                dmModuleTable.setData_mart_id(data_mart_id);
                dmModuleTable.setCategory_id(category_id);
                String table_name = ExcelUtil.getValue(row.getCell(1)).toString();
                List<String> dbNames = dmModuleTableService.findDmModuleTables().stream().map(DmModuleTable::getModule_table_en_name).collect(Collectors.toList());
                if (dbNames.contains(table_name)) {
                    throw new BusinessException(" excel导入时表名：" + table_name + " 已在库中存在");
                }
                dmModuleTable.setModule_table_en_name(table_name);
                saveDmDataTable(dmModuleTable, row, i);
                dmModuleTable.add(Dbo.db());
                DtabRelationStore dtab_relation_store = new DtabRelationStore();
                dtab_relation_store.setTab_id(dmModuleTable.getModule_table_id());
                dtab_relation_store.setDsl_id(dsl_id);
                dtab_relation_store.setIs_successful(JobExecuteState.DengDai.getCode());
                dtab_relation_store.setData_source(StoreLayerDataSource.DM.getCode());
                dtab_relation_store.add(Dbo.db());
                saveColumn(workBook, dmModuleTable, dsl_id);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            if (e.getMessage() != null) {
                throw new BusinessException(e.getMessage());
            } else {
                throw new BusinessException("上传的excel模板存在问题,请检查" + e);
            }
        }
    }

    private void saveColumn(Workbook workBook, DmModuleTable dm_datatable, long dsl_id) {
        DmModuleTableFieldInfo datatable_field_info;
        DcolRelationStore dcol_relation_store;
        Sheet columnSheet = workBook.getSheetAt(1);
        Row columnRow;
        List<DmModuleTableFieldInfo> dmModuleTableFieldInfos = new ArrayList<>();
        for (int j = 1; j <= columnSheet.getLastRowNum(); j++) {
            columnRow = columnSheet.getRow(j);
            if (columnRow == null) {
                break;
            }
            Object tableName = ExcelUtil.getValue(columnRow.getCell(0));
            if (!StringUtil.isBlank(tableName.toString()) && !tableName.toString().equals(dm_datatable.getModule_table_en_name())) {
                continue;
            }
            String field_en_name = ExcelUtil.getValue(columnRow.getCell(3)).toString();
            Validator.notBlank(field_en_name, "第" + j + "行字段英文名不能为空");
            String field_cn_name = ExcelUtil.getValue(columnRow.getCell(2)).toString();
            Validator.notBlank(field_cn_name, "第" + j + "行字段中文名不能为空");
            String field_type_length = ExcelUtil.getValue(columnRow.getCell(4)).toString();
            Validator.notBlank(field_type_length, "第" + j + "行字段类型与精度不能为空");
            String field_type = field_type_length;
            datatable_field_info = new DmModuleTableFieldInfo();
            if (field_type_length.contains(Constant.LXKH) && field_type_length.contains(Constant.RXKH)) {
                String field_length = field_type_length.substring(field_type_length.indexOf(Constant.LXKH) + 1, field_type_length.indexOf(Constant.RXKH));
                datatable_field_info.setField_length(field_length);
                field_type = field_type_length.substring(0, field_type_length.indexOf(Constant.LXKH));
            }
            datatable_field_info.setField_type(field_type);
            datatable_field_info.setModule_field_id(PrimayKeyGener.getNextId());
            datatable_field_info.setModule_table_id(dm_datatable.getModule_table_id());
            datatable_field_info.setField_en_name(field_en_name);
            datatable_field_info.setField_cn_name(field_cn_name);
            String field_seq = ExcelUtil.getValue(columnRow.getCell(1)).toString();
            Validator.notBlank(field_type_length, "第" + j + "行字段序号不能为空");
            datatable_field_info.setField_seq(field_seq);
            datatable_field_info.add(Dbo.db());
            dmModuleTableFieldInfos.add(datatable_field_info);
            dcol_relation_store = new DcolRelationStore();
            dcol_relation_store.setCol_id(datatable_field_info.getModule_field_id());
            dcol_relation_store.setData_source(StoreLayerDataSource.DM.getCode());
            dcol_relation_store.setCsi_number(field_seq);
            String key = ExcelUtil.getValue(columnRow.getCell(5)).toString();
            if (StringUtil.isNotBlank(key) && key.equalsIgnoreCase("Y")) {
                List<Long> dsladIdList = Dbo.queryOneColumnList("select dslad_id from " + DataStoreLayerAdded.TableName + " where dsl_id = ? and dsla_storelayer = ?", dsl_id, StoreLayerAdded.ZhuJian.getCode());
                if (!dsladIdList.isEmpty()) {
                    dcol_relation_store.setDslad_id(dsladIdList.get(0));
                }
            }
            dcol_relation_store.add(Dbo.db());
        }
        if (!dmModuleTableFieldInfos.isEmpty()) {
            DmModuleTableUtil.addHyrenFieldsByModuleFields(dmModuleTableFieldInfos, dm_datatable);
            DmModuleTableUtil.createModuleTableIfNotExistByModuleFields(dsl_id, dmModuleTableFieldInfos, new ArrayList<>(), dm_datatable.getModule_table_en_name());
        }
    }

    private void saveDmDataTable(DmModuleTable dmModuleTable, Row row, int i) {
        String table_ch_name = ExcelUtil.getValue(row.getCell(0)).toString();
        Validator.notBlank(table_ch_name, "第" + i + "行,表中文名不能为空");
        dmModuleTable.setModule_table_cn_name(table_ch_name);
        dmModuleTable.setSql_engine(SqlEngine.SPARK.getCode());
        String datatable_desc = ExcelUtil.getValue(row.getCell(2)).toString();
        dmModuleTable.setModule_table_desc(datatable_desc);
        String storage_type = ExcelUtil.getValue(row.getCell(3)).toString();
        StorageType.ofEnumByCode(storage_type);
        dmModuleTable.setStorage_type(storage_type);
        dmModuleTable.setTable_storage(TableStorage.ShuJuBiao.getCode());
        dmModuleTable.setModule_table_life_cycle(TableLifeCycle.YongJiu.getCode());
        dmModuleTable.setModule_table_d_date(Constant._MAX_DATE_8);
        dmModuleTable.setModule_table_id(PrimayKeyGener.getNextId());
        dmModuleTable.setModule_table_c_date(DateUtil.getSysDate());
        dmModuleTable.setModule_table_c_time(DateUtil.getSysTime());
        dmModuleTable.setDdl_u_date(DateUtil.getSysDate());
        dmModuleTable.setDdl_u_time(DateUtil.getSysTime());
        dmModuleTable.setData_u_date(DateUtil.getSysDate());
        dmModuleTable.setData_u_time(DateUtil.getSysTime());
        dmModuleTable.setModule_table_c_date(DateUtil.getSysDate());
        dmModuleTable.setModule_table_c_time(DateUtil.getSysTime());
        dmModuleTable.setEtl_date(Constant._MAX_DATE_8);
    }

    public void downloadMart(String data_mart_id) {
        String fileName = data_mart_id + ".hrds";
        try (OutputStream out = ContextDataHolder.getResponse().getOutputStream()) {
            ContextDataHolder.getResponse().reset();
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.ISO_8859_1.getValue()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            byte[] bye = getdownloadFile(data_mart_id);
            out.write(bye);
            out.flush();
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new BusinessException("加工工程下载错误" + e.getMessage());
        }
    }

    private byte[] getdownloadFile(String data_mart_id) {
        Map<String, Object> resultmap = new HashMap<>();
        DmInfo dmInfo = new DmInfo();
        dmInfo.setData_mart_id(data_mart_id);
        DmInfo dm_info = Dbo.queryOneObject(DmInfo.class, "select * from " + DmInfo.TableName + " where " + "data_mart_id = ?", dmInfo.getData_mart_id()).orElseThrow(() -> new BusinessException("sql查询错误或者映射实体失败"));
        List<DmCategory> dm_categories = Dbo.queryList(DmCategory.class, "select * from " + DmCategory.TableName + " where data_mart_id =? ", dm_info.getData_mart_id());
        List<DmModuleTable> dmModuleTables = Dbo.queryList(DmModuleTable.class, "select * from " + DmModuleTable.TableName + " where data_mart_id = ?", dm_info.getData_mart_id());
        List<DmModuleTableFieldInfo> datatable_field_infos = Dbo.queryList(DmModuleTableFieldInfo.class, "select * from " + DmModuleTableFieldInfo.TableName + " where module_table_id in " + "(select module_table_id from " + DmModuleTable.TableName + " where data_mart_id =  ? )", dm_info.getData_mart_id());
        List<DmTaskInfo> dmTaskInfos = Dbo.queryList(DmTaskInfo.class, "select * from " + DmTaskInfo.TableName + " where module_table_id in " + "(select module_table_id from " + DmModuleTable.TableName + " where data_mart_id =  ? )", dm_info.getData_mart_id());
        List<DmJobTableInfo> dmJobTableInfos = new ArrayList<>();
        List<DmJobTableFieldInfo> dmJobTableFieldInfos = new ArrayList<>();
        List<DmDatatableSource> dm_datatable_sources = new ArrayList<>();
        List<DmMapInfo> dmMapInfos = new ArrayList<>();
        List<DmOwnSourceField> own_source_fields = new ArrayList<>();
        for (DmTaskInfo dmTaskInfo : dmTaskInfos) {
            List<DmJobTableInfo> dmJobTableInfoList = Dbo.queryList(DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where task_id = ?", dmTaskInfo.getTask_id());
            dmJobTableInfos.addAll(dmJobTableInfoList);
            for (DmJobTableInfo dmJobTableInfo : dmJobTableInfoList) {
                List<DmJobTableFieldInfo> dmJobTableFieldInfoList = Dbo.queryList(DmJobTableFieldInfo.class, "select * from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ?", dmJobTableInfo.getJobtab_id());
                dmJobTableFieldInfos.addAll(dmJobTableFieldInfoList);
                List<DmDatatableSource> dmDatatableSources = Dbo.queryList(DmDatatableSource.class, "select * from " + DmDatatableSource.TableName + " where jobtab_id  = ?", dm_info.getData_mart_id());
                dm_datatable_sources.addAll(dmDatatableSources);
                List<DmMapInfo> dmMapInfoList = Dbo.queryList(DmMapInfo.class, "select * from " + DmMapInfo.TableName + " where jobtab_id = ?", dm_info.getData_mart_id());
                dmMapInfos.addAll(dmMapInfoList);
                List<DmOwnSourceField> dmOwnSourceFields = Dbo.queryList(DmOwnSourceField.class, "select * from " + DmOwnSourceField.TableName + " where own_source_table_id in (" + "select own_source_table_id from " + DmDatatableSource.TableName + " where jobtab_id = ? )", dm_info.getData_mart_id());
                own_source_fields.addAll(dmOwnSourceFields);
            }
        }
        List<DtabRelationStore> dm_relation_datatables = Dbo.queryList(DtabRelationStore.class, "select * from " + DtabRelationStore.TableName + " where tab_id in " + "(select module_table_id from " + DmModuleTable.TableName + " where data_mart_id =  ? ) and data_source =?", dm_info.getData_mart_id(), StoreLayerDataSource.DM.getCode());
        List<DcolRelationStore> dm_column_storages = Dbo.queryList(DcolRelationStore.class, "select * from " + DcolRelationStore.TableName + " where col_id in (" + "select module_field_id from " + DmModuleTableFieldInfo.TableName + " where module_table_id in " + "(select module_table_id from " + DmModuleTable.TableName + " where data_mart_id =  ? )) and data_source = ?", dm_info.getData_mart_id(), StoreLayerDataSource.DM.getCode());
        resultmap.put("dm_info", JsonUtil.toJson(dm_info));
        resultmap.put("dm_datatables", JsonUtil.toJson(dmModuleTables));
        resultmap.put("dmTaskInfos", JsonUtil.toJson(dmTaskInfos));
        resultmap.put("dm_job_table_infos", JsonUtil.toJson(dmJobTableInfos));
        resultmap.put("dm_job_table_field_infos", JsonUtil.toJson(dmJobTableFieldInfos));
        resultmap.put("dm_datatable_sources", JsonUtil.toJson(dm_datatable_sources));
        resultmap.put("dm_etlmap_infos", JsonUtil.toJson(dmMapInfos));
        resultmap.put("own_source_fields", JsonUtil.toJson(own_source_fields));
        resultmap.put("datatable_field_infos", JsonUtil.toJson(datatable_field_infos));
        resultmap.put("dm_relation_datatables", JsonUtil.toJson(dm_relation_datatables));
        resultmap.put("dm_column_storages", JsonUtil.toJson(dm_column_storages));
        resultmap.put("dm_categories", JsonUtil.toJson(dm_categories));
        String martResult = Base64.getEncoder().encodeToString(JsonUtil.toJson(resultmap).getBytes());
        return martResult.getBytes();
    }

    public void getImportFilePath(MultipartFile file) {
        File uploadedFile = null;
        try {
            String originalFilename = file.getOriginalFilename();
            if (null == originalFilename) {
                throw new BusinessException("上传文件不存在！");
            }
            uploadedFile = new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + originalFilename);
            file.transferTo(uploadedFile);
            if (!uploadedFile.exists()) {
                throw new BusinessException("上传文件不存在！");
            }
            String strTemp = new String(Base64.getDecoder().decode(Files.readAllBytes(uploadedFile.toPath())));
            importDataSource(strTemp);
        } catch (IOException e) {
            throw new BusinessException("上传文件失败！");
        } finally {
            if (null != uploadedFile && uploadedFile.exists()) {
                if (!uploadedFile.delete()) {
                    log.error("删除上传文件失败");
                }
            }
        }
    }

    private void importDataSource(String strTemp) {
        Map<String, Object> map = JsonUtil.toObject(strTemp, new TypeReference<Map<String, Object>>() {
        });
        String dm_info = ReqDataUtils.getStringData(map, "dm_info");
        DmInfo dmInfo = JsonUtil.toObject(dm_info, new TypeReference<DmInfo>() {
        });
        List<DmInfo> dmInfos = dmInfoService.findDmInfos();
        List<Long> collect = dmInfos.stream().map(DmInfo::getData_mart_id).collect(Collectors.toList());
        if (collect.contains(dmInfo.getData_mart_id())) {
            throw new BusinessException("此工程已存在！请勿重复导入！");
        }
        dmInfo.add(Dbo.db());
        String dm_datatables = ReqDataUtils.getStringData(map, "dm_datatables");
        List<DmModuleTable> dmModuleTables = JsonUtil.toObject(dm_datatables, new TypeReference<List<DmModuleTable>>() {
        });
        for (DmModuleTable dmModuleTable : dmModuleTables) {
            dmModuleTable.add(Dbo.db());
        }
        String dmTaskInfos = ReqDataUtils.getStringData(map, "dmTaskInfos");
        List<DmTaskInfo> dmTaskInfoList = JsonUtil.toObject(dmTaskInfos, new TypeReference<List<DmTaskInfo>>() {
        });
        for (DmTaskInfo dmTaskInfo : dmTaskInfoList) {
            dmTaskInfo.add(Dbo.db());
        }
        String dm_job_table_infos = ReqDataUtils.getStringData(map, "dm_job_table_infos");
        List<DmJobTableInfo> dmJobTableInfos = JsonUtil.toObject(dm_job_table_infos, new TypeReference<List<DmJobTableInfo>>() {
        });
        for (DmJobTableInfo dmJobTableInfo : dmJobTableInfos) {
            dmJobTableInfo.add(Dbo.db());
        }
        String dm_job_table_field_infos = ReqDataUtils.getStringData(map, "dm_job_table_field_infos");
        List<DmJobTableFieldInfo> dmJobTableFieldInfos = JsonUtil.toObject(dm_job_table_field_infos, new TypeReference<List<DmJobTableFieldInfo>>() {
        });
        for (DmJobTableFieldInfo dmJobTableFieldInfo : dmJobTableFieldInfos) {
            dmJobTableFieldInfo.add(Dbo.db());
        }
        String dm_datatable_sources = ReqDataUtils.getStringData(map, "dm_datatable_sources");
        List<DmDatatableSource> dmDatatableSources = JsonUtil.toObject(dm_datatable_sources, new TypeReference<List<DmDatatableSource>>() {
        });
        for (DmDatatableSource dmDatatableSource : dmDatatableSources) {
            dmDatatableSource.add(Dbo.db());
        }
        String dm_etlmap_infos = ReqDataUtils.getStringData(map, "dm_etlmap_infos");
        List<DmMapInfo> dmMapInfos = JsonUtil.toObject(dm_etlmap_infos, new TypeReference<List<DmMapInfo>>() {
        });
        for (DmMapInfo dmMapInfo : dmMapInfos) {
            dmMapInfo.add(Dbo.db());
        }
        String own_source_fields = ReqDataUtils.getStringData(map, "own_source_fields");
        List<DmOwnSourceField> dmOwnSourceFields = JsonUtil.toObject(own_source_fields, new TypeReference<List<DmOwnSourceField>>() {
        });
        for (DmOwnSourceField dmOwnSourceField : dmOwnSourceFields) {
            dmOwnSourceField.add(Dbo.db());
        }
        String datatable_field_infos = ReqDataUtils.getStringData(map, "datatable_field_infos");
        List<DmModuleTableFieldInfo> dmModuleTableFieldInfos = JsonUtil.toObject(datatable_field_infos, new TypeReference<List<DmModuleTableFieldInfo>>() {
        });
        for (DmModuleTableFieldInfo dmModuleTableFieldInfo : dmModuleTableFieldInfos) {
            dmModuleTableFieldInfo.add(Dbo.db());
        }
        String dm_relation_datatables = ReqDataUtils.getStringData(map, "dm_relation_datatables");
        List<DtabRelationStore> dtabRelationStores = JsonUtil.toObject(dm_relation_datatables, new TypeReference<List<DtabRelationStore>>() {
        });
        for (DtabRelationStore dtabRelationStore : dtabRelationStores) {
            dtabRelationStore.add(Dbo.db());
        }
        String dm_column_storages = ReqDataUtils.getStringData(map, "dm_column_storages");
        List<DcolRelationStore> dcolRelationStores = JsonUtil.toObject(dm_column_storages, new TypeReference<List<DcolRelationStore>>() {
        });
        for (DcolRelationStore dcolRelationStore : dcolRelationStores) {
            dcolRelationStore.add(Dbo.db());
        }
        String dm_categories = ReqDataUtils.getStringData(map, "dm_categories");
        List<DmCategory> dmCategories = JsonUtil.toObject(dm_categories, new TypeReference<List<DmCategory>>() {
        });
        for (DmCategory dmCategory : dmCategories) {
            dmCategory.add(Dbo.db());
        }
    }

    public void startTask(Long module_table_id) {
        String shPath = System.getProperty("user.dir") + File.separator + FLINK_JOB_SH;
        File shFile = new File(shPath);
        if (!shFile.isFile()) {
            throw new BusinessException("未找到 sh 脚本：" + shPath);
        }
        DatabaseWrapper db = Dbo.db();
        List<DmJobTableInfo> jobList = Dbo.queryList(db, DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where MODULE_TABLE_ID = ?", module_table_id);
        Validator.notEmpty(jobList, "未找到job");
        for (DmJobTableInfo job : jobList) {
            ProcessBuilder processBuilder = new ProcessBuilder("bash", shPath, module_table_id.toString(), job.getJobtab_id().toString(), job.getJobtab_en_name(), " -P hyrenJob", " -J " + job.getJobtab_en_name(), " -S dataMarket", " -E default", " -D");
            log.info(Arrays.toString(processBuilder.command().toArray()));
            Process process;
            try {
                process = processBuilder.start();
                int exitCode = process.waitFor();
                log.info("脚本执行完成，退出码：" + exitCode);
            } catch (IOException e) {
                throw new AppSystemException("脚本执行异常，退出 IOException: " + e);
            } catch (InterruptedException e) {
                log.error("脚本执行完成，退出 InterruptedException：" + e);
                throw new AppSystemException("脚本执行异常，退出 IOException: " + e);
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Map<String, List<Object>> getSparkSqlGram() {
        List<EdwSparksqlGram> sparkSqlGramsList = Dbo.queryList(EdwSparksqlGram.class, "select * from " + EdwSparksqlGram.TableName + " ORDER BY function_name");
        Map<String, List<Object>> sparkSqlGramsMap = new LinkedHashMap<>();
        List<Object> classifyList = new ArrayList<>();
        sparkSqlGramsList.forEach(itemBean -> {
            if (sparkSqlGramsMap.containsKey(itemBean.getFunction_classify())) {
                sparkSqlGramsMap.get(itemBean.getFunction_classify()).add(itemBean);
            } else {
                classifyList.add(itemBean.getFunction_classify());
                List<Object> itemList = new ArrayList<>();
                itemList.add(itemBean);
                sparkSqlGramsMap.put(itemBean.getFunction_classify(), itemList);
            }
        });
        sparkSqlGramsMap.put("classify", classifyList);
        return sparkSqlGramsMap;
    }

    public void downloadMarketExcel() {
        String filePath = System.getProperty("user.dir") + File.separator + "dmModuleImportExcel.xlsx";
        File file = new File(filePath);
        if (!file.exists()) {
            throw new BusinessException(String.format("加工Excel模板不存在，filePath:%s", filePath));
        }
        FileDownloadUtil.downloadFile(filePath);
    }
}
