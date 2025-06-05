package hyren.serv6.s.datastore;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.CodecUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.ContextDataHolder;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.utils.Aes.AesUtil;
import hyren.serv6.base.utils.fileutil.FileUploadUtil;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.collection.bean.JDBCBean;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.commons.solr.param.SolrParam;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.database.DatabaseConnUtil;
import hyren.serv6.commons.utils.database.bean.DBConnectionProp;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.commons.utils.xlstoxml.util.ExcelUtil;
import hyren.serv6.s.bean.StoreConnectionBean;
import hyren.serv6.s.driver.DriverImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DataStoreService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchDataStore(String sql, Object... params) {
        return Dbo.queryResult(Dbo.db(), sql, params);
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchDBName(String sql, Object... params) {
        return Dbo.queryResult(Dbo.db(), sql, params);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "store_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getDataLayerAttrKey(String store_type) {
        String value = Store_type.ofValueByCode(store_type);
        Map<String, List<String>> storageKeys = StorageTypeKey.getFinallyStorageKeys();
        List<String> updateStorageKeys = StorageTypeKey.getUpdateFinallyStorageKeys();
        List<String> keyList;
        if (Store_type.DATABASE == Store_type.ofEnumByCode(store_type)) {
            keyList = storageKeys.get(store_type);
        } else {
            keyList = storageKeys.get(value);
        }
        return getAttrKeyByIsFile(updateStorageKeys, keyList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "updateStorageKeys", desc = "", range = "")
    @Param(name = "keyList", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, Object> getAttrKeyByIsFile(List<String> updateStorageKeys, List<String> keyList) {
        List<String> fileKey = new ArrayList<>();
        List<String> jdbcKey = new ArrayList<>();
        if (keyList != null) {
            for (String key : keyList) {
                if (updateStorageKeys.contains(key)) {
                    fileKey.add(key);
                } else {
                    jdbcKey.add(key);
                }
            }
        }
        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put("jdbcKey", jdbcKey);
        keyMap.put("fileKey", fileKey);
        return keyMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db_name", desc = "", range = "")
    @Param(name = "port", desc = "", range = "", nullable = true, valueIfNull = "")
    @Return(desc = "", range = "", isBean = true)
    public DBConnectionProp getDBConnectionMsg(String db_name, String port) {
        return DatabaseConnUtil.getConnParamInfo(db_name, port);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_name", desc = "", range = "")
    @Param(name = "store_type", desc = "", range = "")
    @Param(name = "is_hadoopclient", desc = "", range = "")
    @Param(name = "dsl_remark", desc = "", range = "", nullable = true)
    @Param(name = "dsl_source", desc = "", range = "")
    @Param(name = "dsl_goal", desc = "", range = "")
    @Param(name = "dataStoreLayerAttr", desc = "", range = "")
    @Param(name = "dsla_storelayer", desc = "", range = "", nullable = true)
    @Param(name = "files", desc = "", range = "", nullable = true)
    public void addDataStore(String dsl_name, String store_type, String is_hadoopclient, String dsl_remark, String dsl_source, String dsl_goal, String dataStoreLayerAttr, String[] dsla_storelayer, MultipartFile[] files) {
        DataStoreLayer dataStoreLayer = new DataStoreLayer();
        isDslNameExist(dsl_name);
        dataStoreLayer.setDsl_name(dsl_name);
        dataStoreLayer.setIs_hadoopclient(is_hadoopclient);
        dataStoreLayer.setDsl_remark(dsl_remark);
        dataStoreLayer.setStore_type(store_type);
        dataStoreLayer.setDsl_source(dsl_source);
        dataStoreLayer.setDsl_goal(dsl_goal);
        List<Map<String, String>> maps = getMaps(dataStoreLayerAttr);
        maps.forEach(map -> {
            if (map.get("storage_property_key").equals(StorageTypeKey.database_type)) {
                dataStoreLayer.setDatabase_name(map.get("storage_property_val"));
            }
        });
        checkDataStorageField(dataStoreLayer);
        dataStoreLayer.setDsl_id(PrimayKeyGener.getNextId());
        dataStoreLayer.add(Dbo.db());
        if (dsla_storelayer != null && dsla_storelayer.length != 0) {
            addDataStoreLayerAdded(dataStoreLayer.getDsl_id(), dsla_storelayer);
        }
        addDataStorageLayerAttr(dataStoreLayerAttr, dataStoreLayer.getDsl_id(), store_type);
        if (files != null && files.length != 0) {
            uploadConfFile(files, dataStoreLayer.getDsl_id(), dsl_name, dataStoreLayerAttr);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_name", desc = "", range = "")
    private void isDslNameExist(String dsl_name) {
        if (Dbo.queryNumber("select count(1) from " + DataStoreLayer.TableName + " where dsl_name=?", dsl_name).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            throw new BusinessException("存储层配置属性名称不能重复");
        }
    }

    private List<Map<String, String>> getMaps(String dataStoreLayerAttr) {
        return JsonUtil.toObjectSafety(dataStoreLayerAttr, List.class).orElseThrow(() -> new BusinessException("数据存储层信息属性信息转List集合时发生异常!"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dataStoreLayer", desc = "", range = "", isBean = true)
    private void checkDataStorageField(DataStoreLayer dataStoreLayer) {
        Validator.notBlank(dataStoreLayer.getDsl_name(), "配置属性名称不能为空！");
        Store_type.ofEnumByCode(dataStoreLayer.getStore_type());
        IsFlag.ofEnumByCode(dataStoreLayer.getIs_hadoopclient());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "dslad_remark", desc = "", range = "", nullable = true)
    @Param(name = "dsla_storelayer", desc = "", range = "", nullable = true)
    private void addDataStoreLayerAdded(long dsl_id, String[] dsla_storelayer) {
        if (dsla_storelayer != null) {
            for (String dslaStorelayer : dsla_storelayer) {
                DataStoreLayerAdded dataStoreLayerAdded = new DataStoreLayerAdded();
                dataStoreLayerAdded.setDsl_id(dsl_id);
                dataStoreLayerAdded.setDslad_id(PrimayKeyGener.getNextId());
                dataStoreLayerAdded.setDsla_storelayer(dslaStorelayer);
                checkDataStoreLayerAddedField(dataStoreLayerAdded);
                dataStoreLayerAdded.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @PostMapping("/uploadExcelFile")
    public void uploadExcelFile(String file) {
        Workbook workBook = null;
        try {
            File uploadedFile = FileUploadUtil.getUploadedFile(file);
            if (!uploadedFile.exists()) {
                throw new BusinessException("上传文件不存在！");
            }
            workBook = ExcelUtil.getWorkbookFromExcel(uploadedFile);
            int numberOfSheets = Objects.requireNonNull(workBook).getNumberOfSheets();
            List<List<String>> dataList = new ArrayList<>();
            for (int sheetNum = 0; sheetNum < numberOfSheets; sheetNum++) {
                Sheet sheet = workBook.getSheetAt(sheetNum);
                Row row;
                String cellVal;
                int physicalNumberOfRows = sheet.getPhysicalNumberOfRows();
                for (int i = sheet.getFirstRowNum() + 1; i < physicalNumberOfRows; i++) {
                    List<String> list = new ArrayList<>();
                    row = sheet.getRow(i);
                    int physicalNumberOfCells = sheet.getRow(0).getPhysicalNumberOfCells();
                    for (int j = sheet.getRow(0).getFirstCellNum(); j < physicalNumberOfCells; j++) {
                        if (j == -1) {
                            continue;
                        }
                        Cell cell = row.getCell(j);
                        if (null == cell) {
                            cellVal = "";
                        } else {
                            cellVal = cell.toString();
                        }
                        list.add(cellVal);
                    }
                    dataList.add(list);
                }
            }
            for (List<String> dataInfo : dataList) {
                try (DatabaseWrapper db = new DatabaseWrapper()) {
                    String db_name1 = dataInfo.get(0);
                    Validator.notBlank(db_name1, "数据库名称不能为空!");
                    String db_name2 = dataInfo.get(1);
                    Validator.notBlank(db_name2, "数据库名称不能为空!");
                    String data_type1 = dataInfo.get(2);
                    Validator.notBlank(data_type1, "数据库类型不能为空!");
                    String data_type2 = dataInfo.get(3);
                    Validator.notBlank(data_type2, "数据库类型不能为空!");
                    DatabaseInfo databaseInfo = new DatabaseInfo();
                    long ori_execute = SqlOperator.queryNumber(db, "select count(1) from " + DatabaseInfo.TableName + " where database_name = ? ", db_name1).orElseThrow(() -> new BusinessException("sql查询错误"));
                    if (ori_execute == 0) {
                        databaseInfo.setDatabase_name(db_name1);
                        databaseInfo.add(db);
                    }
                    long tar_execute = SqlOperator.queryNumber(db, "select count(1) from " + DatabaseInfo.TableName + " where database_name = ? ", db_name2).orElseThrow(() -> new BusinessException("sql查询错误"));
                    if (tar_execute == 0) {
                        databaseInfo.setDatabase_name(db_name2);
                        databaseInfo.add(db);
                    }
                    Result result = SqlOperator.queryResult(db, "select * from " + DatabaseTypeMapping.TableName + " where database_name1 = ? and database_name2 = ? and database_type1 = ? ", db_name1, db_name2, data_type1);
                    if (!result.isEmpty()) {
                        for (int i = 0; i < result.getRowCount(); i++) {
                            int execute = SqlOperator.execute(db, "update " + DatabaseTypeMapping.TableName + " set database_type1 = ?" + ",database_type2 = ? where dtm_id = ?", result.getString(i, "database_type1"), data_type2, Long.parseLong(result.getString(i, "dtm_id")));
                            if (execute < 1) {
                                throw new BusinessException("更新数据库表数据类型对应关系失败!");
                            }
                        }
                    } else {
                        DatabaseTypeMapping db_type = new DatabaseTypeMapping();
                        db_type.setDtm_id(PrimayKeyGener.getNextId());
                        db_type.setDatabase_name1(db_name1);
                        db_type.setDatabase_name2(db_name2);
                        db_type.setDatabase_type1(data_type1);
                        db_type.setDatabase_type2(data_type2);
                        db_type.setDtm_remark(dataInfo.get(4));
                        db_type.add(db);
                    }
                    db.commit();
                }
            }
        } catch (FileNotFoundException e) {
            throw new BusinessException("导入excel文件数据失败！");
        } catch (IOException e) {
            throw new BusinessException("获取excel对象失败，文件类型错误");
        } finally {
            try {
                if (workBook != null) {
                    workBook.close();
                }
            } catch (IOException e) {
                log.error(String.valueOf(e));
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "store_type", desc = "", range = "")
    @Param(name = "is_hadoopclient", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getLayerAttrByIdAndType(long dsl_id, String store_type, String is_hadoopclient) {
        Store_type.ofEnumByCode(store_type);
        IsFlag.ofEnumByCode(is_hadoopclient);
        return Dbo.queryResult("select t2.* from " + DataStoreLayer.TableName + " t1 left join " + DataStoreLayerAttr.TableName + " t2 on t1.dsl_id=t2.dsl_id " + " where t1.dsl_id=? and t1.store_type=? and is_hadoopclient=?", dsl_id, store_type, is_hadoopclient);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    @Param(name = "filePath", desc = "", range = "")
    @PostMapping("/downloadConfFile")
    public void downloadConfFile(String fileName, String filePath) {
        OutputStream out = null;
        InputStream in = null;
        try {
            ContextDataHolder.getResponse().reset();
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.ISO_8859_1.getValue()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            in = new FileInputStream(filePath);
            out = ContextDataHolder.getResponse().getOutputStream();
            byte[] bytes = new byte[1024];
            int len;
            while ((len = in.read(bytes)) > 0) {
                out.write(bytes, 0, len);
            }
            out.flush();
            out.close();
            in.close();
        } catch (UnsupportedEncodingException e) {
            throw new BusinessException(String.format("不支持的编码异常:%s", e));
        } catch (FileNotFoundException e) {
            throw new BusinessException(String.format("文件不存在，可能目录不存在:%s", e));
        } catch (IOException e) {
            throw new BusinessException(String.format("下载文件失败:%s", e));
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (Exception e) {
                log.error(String.valueOf(e));
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e) {
                log.error(String.valueOf(e));
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dataStoreLayerAdded", desc = "", range = "", isBean = true)
    private void checkDataStoreLayerAddedField(DataStoreLayerAdded dataStoreLayerAdded) {
        Validator.notNull(dataStoreLayerAdded.getDsl_id(), "存储层配置ID不能为空");
        Validator.notNull(dataStoreLayerAdded.getDsla_storelayer(), "配置附加属性信息不能为空");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dataStoreLayerAttr", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "store_type", desc = "", range = "")
    private void addDataStorageLayerAttr(String dataStoreLayerAttr, long dsl_id, String store_type) {
        List<Map<String, String>> layerAttrList = getMaps(dataStoreLayerAttr);
        DataStoreLayerAttr data_store_layer_attr = new DataStoreLayerAttr();
        Store_type storeType = Store_type.ofEnumByCode(store_type);
        if (Store_type.DATABASE != storeType) {
            data_store_layer_attr.setDsla_id(PrimayKeyGener.getNextId());
            data_store_layer_attr.setDsl_id(dsl_id);
            data_store_layer_attr.setStorage_property_key(StorageTypeKey.database_type);
            data_store_layer_attr.setStorage_property_val(storeType.getValue());
            data_store_layer_attr.setIs_file(IsFlag.Fou.getCode());
            data_store_layer_attr.add(Dbo.db());
        }
        for (Map<String, String> layerAttr : layerAttrList) {
            String is_file = String.valueOf(layerAttr.get("is_file"));
            if (IsFlag.Fou == IsFlag.ofEnumByCode(is_file)) {
                data_store_layer_attr.setDsla_id(PrimayKeyGener.getNextId());
                data_store_layer_attr.setDsl_id(dsl_id);
                data_store_layer_attr.setDsla_remark(layerAttr.get("dsla_remark"));
                data_store_layer_attr.setIs_file(is_file);
                data_store_layer_attr.setStorage_property_key(layerAttr.get("storage_property_key"));
                data_store_layer_attr.setStorage_property_val(layerAttr.get("storage_property_val"));
                checkDataStoreLayerAttrField(data_store_layer_attr);
                data_store_layer_attr.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dataStoreLayerAttr", desc = "", range = "", isBean = true)
    private void checkDataStoreLayerAttrField(DataStoreLayerAttr dataStoreLayerAttr) {
        Validator.notNull(dataStoreLayerAttr.getDsl_id(), "存储层配置ID不能为空");
        Validator.notNull(dataStoreLayerAttr.getStorage_property_key(), "属性key不能为空");
        Validator.notBlank(dataStoreLayerAttr.getStorage_property_val(), "属性value不能为空");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "files", desc = "", range = "", nullable = true)
    @Param(name = "dsla_remark", desc = "", range = "", nullable = true)
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "dataStoreLayerAttr", desc = "", range = "")
    private void uploadConfFile(MultipartFile[] files, long dsl_id, String dsl_name, String dataStoreLayerAttr) {
        DataStoreLayerAttr data_store_layer_attr = new DataStoreLayerAttr();
        List<Map<String, String>> attrList = getMaps(dataStoreLayerAttr);
        attrList = attrList.stream().filter(map -> IsFlag.Shi == IsFlag.ofEnumByCode(map.get("is_file"))).collect(Collectors.toList());
        for (MultipartFile file : files) {
            data_store_layer_attr.setDsla_id(PrimayKeyGener.getNextId());
            data_store_layer_attr.setDsl_id(dsl_id);
            for (Map<String, String> map : attrList) {
                String storage_property_val = map.get("storage_property_val");
                String originalFileName = file.getOriginalFilename();
                if (storage_property_val.equals(originalFileName)) {
                    data_store_layer_attr.setStorage_property_key(map.get("storage_property_key"));
                    File destFileDir = new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + dsl_name);
                    if (!destFileDir.exists() && !destFileDir.isDirectory()) {
                        if (!destFileDir.mkdirs()) {
                            throw new BusinessException("创建文件目录失败");
                        }
                    }
                    String pathname = destFileDir.getPath() + File.separator + originalFileName;
                    File destFile = new File(pathname);
                    try {
                        file.transferTo(destFile.toPath());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    data_store_layer_attr.setStorage_property_val(destFile.getPath());
                    data_store_layer_attr.setIs_file(IsFlag.Shi.getCode());
                    data_store_layer_attr.setDsla_remark(originalFileName + "文件已上传");
                    data_store_layer_attr.add(Dbo.db());
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public String searchDataStoreById(long dsl_id) {
        Map<String, Object> storeLayer = Dbo.queryOneObject("select * from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id);
        List<Map<String, Object>> layerAndAdded = Dbo.queryList("select * from " + DataStoreLayerAdded.TableName + " where dsl_id=?", dsl_id);
        List<Map<String, Object>> layerAndAttr = Dbo.queryList("select * from " + DataStoreLayerAttr.TableName + " where dsl_id=? order by is_file", dsl_id);
        storeLayer.put("layerAndAdded", layerAndAdded);
        storeLayer.put("layerAndAttr", layerAndAttr);
        return AesUtil.encrypt(JsonUtil.toJson(storeLayer));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_name", desc = "", range = "")
    @Param(name = "store_type", desc = "", range = "")
    @Param(name = "is_hadoopclient", desc = "", range = "")
    @Param(name = "dsl_remark", desc = "", range = "", nullable = true)
    @Param(name = "dsl_source", desc = "", range = "")
    @Param(name = "dsl_goal", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "dataStoreLayerAttr", desc = "", range = "")
    @Param(name = "dsla_storelayer", desc = "", range = "", nullable = true)
    @Param(name = "files", desc = "", range = "", nullable = true)
    public void updateDataStore(String dsl_name, String store_type, String is_hadoopclient, String dsl_remark, String dsl_source, String dsl_goal, long dsl_id, String dataStoreLayerAttr, String[] dsla_storelayer, MultipartFile[] files) {
        DataStoreLayer dataStoreLayer = new DataStoreLayer();
        List<String> dslNameList = Dbo.queryOneColumnList("select dsl_name from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id);
        if (dslNameList.isEmpty()) {
            throw new BusinessException("当前存储层已不存在：" + dsl_id);
        }
        if (!dslNameList.get(0).equals(dsl_name)) {
            isDslNameExist(dsl_name);
        }
        dataStoreLayer.setDsl_name(dsl_name);
        dataStoreLayer.setIs_hadoopclient(is_hadoopclient);
        dataStoreLayer.setDsl_remark(dsl_remark);
        dataStoreLayer.setStore_type(store_type);
        dataStoreLayer.setDsl_id(dsl_id);
        dataStoreLayer.setDsl_source(dsl_source);
        dataStoreLayer.setDsl_goal(dsl_goal);
        checkDataStorageField(dataStoreLayer);
        List<Map<String, String>> maps = getMaps(dataStoreLayerAttr);
        maps.forEach(map -> {
            if (map.get("storage_property_key").equals(StorageTypeKey.database_type)) {
                dataStoreLayer.setDatabase_name(map.get("storage_property_val"));
            }
        });
        dataStoreLayer.update(Dbo.db());
        deleteDataStoreLayerAdded(dsl_id);
        addDataStoreLayerAdded(dsl_id, dsla_storelayer);
        updateDataStorageLayerAttr(dataStoreLayerAttr, dsl_id, store_type);
        if (files != null && files.length != 0) {
            deleteConfFile(dsl_id, files);
            uploadConfFile(files, dsl_id, dsl_name, dataStoreLayerAttr);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    private void deleteDataStoreLayerAdded(long dsl_id) {
        Dbo.execute("delete from " + DataStoreLayerAdded.TableName + " where dsl_id=?", dsl_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dataStoreLayerAttr", desc = "", range = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "store_type", desc = "", range = "")
    private void updateDataStorageLayerAttr(String dataStoreLayerAttr, long dsl_id, String store_type) {
        deleteDataStoreLayerAttr(dsl_id);
        addDataStorageLayerAttr(dataStoreLayerAttr, dsl_id, store_type);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    private void deleteDataStoreLayerAttr(long dsl_id) {
        Dbo.execute("delete from " + DataStoreLayerAttr.TableName + " where dsl_id = ? and is_file = ?", dsl_id, IsFlag.Fou.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    private void deleteConfFile(long dsl_id, MultipartFile[] files) {
        for (MultipartFile file : files) {
            String fileName = file.getOriginalFilename();
            Optional<DataStoreLayerAttr> dataStoreLayerAttr = Dbo.queryOneObject(DataStoreLayerAttr.class, "select * from " + DataStoreLayerAttr.TableName + " where dsl_id=? and storage_property_key=? and is_file=?", dsl_id, fileName, IsFlag.Shi.getCode());
            if (dataStoreLayerAttr.isPresent()) {
                DataStoreLayerAttr storeLayerAttr = dataStoreLayerAttr.get();
                if (null != fileName && fileName.equals(storeLayerAttr.getStorage_property_key())) {
                    if (new File(storeLayerAttr.getStorage_property_val()).exists()) {
                        try {
                            Files.delete(new File(storeLayerAttr.getStorage_property_val()).toPath());
                        } catch (IOException e) {
                            throw new BusinessException("删除文件失败！");
                        }
                    }
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    public void deleteDataStore(long dsl_id) {
        DboExecute.deletesOrThrow("删除data_store_layer表信息失败，dsl_id=" + dsl_id, "delete from " + DataStoreLayer.TableName + " where dsl_id=?", dsl_id);
        deleteDataStoreLayerAdded(dsl_id);
        deleteDataStoreLayerAttr(dsl_id);
        List<String> storagePropertyValList = Dbo.queryOneColumnList("select storage_property_val from " + DataStoreLayerAttr.TableName + " where is_file=? and dsl_id=?", IsFlag.Shi.getCode(), dsl_id);
        for (String storagePropertyVal : storagePropertyValList) {
            if (new File(storagePropertyVal).exists()) {
                try {
                    Files.delete(new File(storagePropertyVal).toPath());
                } catch (IOException e) {
                    throw new BusinessException("删除文件失败！");
                }
            }
        }
        Dbo.execute("delete from " + DataStoreLayerAttr.TableName + " where dsl_id=?" + " and is_file=?", dsl_id, IsFlag.Shi.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchContrastTypeInfo() {
        Result result = Dbo.queryResult("select database_name1,database_name2 from " + DatabaseTypeMapping.TableName + " GROUP BY database_name1,database_name2");
        for (int i = 0; i < result.getRowCount(); i++) {
            long count = Dbo.queryNumber("select count(*) from " + DatabaseTypeMapping.TableName + " where database_name1 = ? and database_name2 = ?", result.getString(i, "database_name1"), result.getString(i, "database_name2")).orElseThrow(() -> new BusinessException("SQL查询错误"));
            result.setValue(i, "data_type_count", String.valueOf(count));
        }
        return result;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "store_type", desc = "", range = "")
    @Param(name = "is_hadoopclient", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getExternalTableAttrKey(String store_type, String is_hadoopclient) {
        String value = Store_type.ofValueByCode(store_type);
        if (IsFlag.ofEnumByCode(is_hadoopclient) != IsFlag.Shi) {
            throw new BusinessException("是否支持外部表应该选是");
        }
        Map<String, List<String>> storageKeys = StorageTypeKey.getFinallyStorageKeys();
        List<String> updateStorageKeys = StorageTypeKey.getUpdateFinallyStorageKeys();
        List<String> keyList;
        if (Store_type.DATABASE == Store_type.ofEnumByCode(store_type)) {
            keyList = storageKeys.get(store_type + "_" + IsFlag.Shi.getCode());
        } else {
            keyList = storageKeys.get(value + "_" + IsFlag.Shi.getCode());
        }
        return getAttrKeyByIsFile(updateStorageKeys, keyList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "store_type", desc = "", range = "")
    @Param(name = "is_hadoopclient", desc = "", range = "")
    @Param(name = "db_name", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAttrKeyByDatabaseType")
    public Map<String, Object> getAttrKeyByDatabaseType(String store_type, String is_hadoopclient, String db_name) {
        Map<String, List<String>> storageKeys = StorageTypeKey.getFinallyStorageKeys();
        if (IsFlag.Shi == IsFlag.ofEnumByCode(is_hadoopclient)) {
            List<String> updateStorageKeys = StorageTypeKey.getUpdateFinallyStorageKeys();
            List<String> keyList = storageKeys.get(db_name + "_" + IsFlag.Shi.getCode());
            if (null == keyList) {
                return getDataLayerAttrKey(store_type);
            } else {
                return getAttrKeyByIsFile(updateStorageKeys, keyList);
            }
        } else {
            throw new BusinessException("是否支持外部表请选择是！");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db_name1", desc = "", range = "")
    @Param(name = "db_name2", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getDataTypeMsg(String db_name1, String db_name2) {
        return Dbo.queryList("select database_type1,database_type2 from " + DatabaseTypeMapping.TableName + " where database_name1 = ? and database_name2 = ?", db_name1, db_name2);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void generateExcel(String fileName) {
        try {
            List<DatabaseTypeMapping> data = Dbo.queryList(DatabaseTypeMapping.class, "select * from " + DatabaseTypeMapping.TableName);
            XSSFWorkbook wb = new XSSFWorkbook();
            XSSFSheet sheet1 = wb.createSheet("数据库字段类型对照表");
            XSSFCellStyle style = wb.createCellStyle();
            style.setFillForegroundColor((short) 13);
            style.setBorderBottom(BorderStyle.THICK);
            style.setBorderLeft(BorderStyle.THICK);
            style.setBorderTop(BorderStyle.THICK);
            style.setBorderRight(BorderStyle.THICK);
            style.setAlignment(HorizontalAlignment.CENTER);
            XSSFRow rowHead1 = sheet1.createRow(1);
            XSSFCell cellHead1 = rowHead1.createCell(0);
            cellHead1.setCellStyle(style);
            XSSFRow row1 = sheet1.createRow(0);
            XSSFCell cell1 = row1.createCell(0);
            cell1.setCellValue("数据库名称1(不可为空)");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(1);
            cell1.setCellValue("数据库名称2(不可为空)");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(2);
            cell1.setCellValue("数据库字段类型1(不可为空)");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(3);
            cell1.setCellValue("数据库字段类型2(不可为空)");
            cell1.setCellStyle(style);
            cell1 = row1.createCell(4);
            cell1.setCellValue("备注信息");
            cell1.setCellStyle(style);
            if (!data.isEmpty()) {
                for (int i = 0; i < data.size(); i++) {
                    row1 = sheet1.createRow(i + 1);
                    row1.createCell(0).setCellValue(i + 1);
                    row1.createCell(0).setCellValue(data.get(i).getDatabase_name1());
                    row1.createCell(1).setCellValue(data.get(i).getDatabase_name2());
                    row1.createCell(2).setCellValue(data.get(i).getDatabase_type1());
                    row1.createCell(3).setCellValue(data.get(i).getDatabase_type2());
                    row1.createCell(4).setCellValue(data.get(i).getDtm_remark());
                    for (int j = 0; j <= cell1.getColumnIndex(); j++) {
                        Cell cell = row1.getCell(j);
                        cell.setCellStyle(style);
                    }
                }
            }
            sheet1.setColumnWidth(0, 6000);
            sheet1.setColumnWidth(1, 6000);
            sheet1.setColumnWidth(2, 6000);
            sheet1.setColumnWidth(3, 6000);
            sheet1.setColumnWidth(4, 10000);
            FileOutputStream fout = new FileOutputStream(WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName);
            wb.write(fout);
            fout.close();
        } catch (FileNotFoundException e) {
            log.info("文件异常!");
            throw new BusinessException("文件不存在！" + e.getMessage());
        } catch (IOException e) {
            log.info("流转化异常!");
            throw new BusinessException("生成excel文件失败！" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fileName", desc = "", range = "")
    public void downloadFile(String fileName) {
        OutputStream out = null;
        InputStream in = null;
        try {
            String filePath = WebinfoProperties.FileUpload_SavedDirName + File.separator + fileName;
            log.info("=====本地下载文件路径=====" + filePath);
            ContextDataHolder.getResponse().reset();
            ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            if (ContextDataHolder.getRequest().getHeader("User-Agent").toLowerCase().indexOf("firefox") > 0) {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + new String(fileName.getBytes(CodecUtil.UTF8_CHARSET), DataBaseCode.ISO_8859_1.getCode()));
            } else {
                ContextDataHolder.getResponse().setHeader("content-disposition", "attachment;filename=" + Base64.getEncoder().encodeToString(fileName.getBytes(CodecUtil.UTF8_CHARSET)));
            }
            ContextDataHolder.getResponse().setContentType("APPLICATION/OCTET-STREAM");
            in = new FileInputStream(filePath);
            out = ContextDataHolder.getResponse().getOutputStream();
            byte[] bytes = new byte[1024];
            int len;
            while ((len = in.read(bytes)) > 0) {
                out.write(bytes, 0, len);
            }
            out.flush();
        } catch (UnsupportedEncodingException e) {
            throw new BusinessException("不支持的编码异常");
        } catch (FileNotFoundException e) {
            throw new BusinessException("文件不存在，可能目录不存在！");
        } catch (IOException e) {
            throw new BusinessException("下载文件失败！");
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    log.error(String.valueOf(e));
                }
            }
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    log.error(String.valueOf(e));
                }
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "jdbcBean", desc = "", range = "")
    public void testConnection(StoreConnectionBean storeConnectionBean) {
        Store_type storeType = Store_type.ofEnumByCode(storeConnectionBean.getStore_type());
        if (Store_type.DATABASE == storeType) {
            Validator.notBlank(storeConnectionBean.getDatabase_type(), "数据库类型不能为空");
            checkJdbcParams(storeConnectionBean);
            testJdbcConnection(storeConnectionBean);
        } else if (Store_type.HIVE == storeType) {
            checkJdbcParams(storeConnectionBean);
            storeConnectionBean.setDatabase_type(Store_type.HIVE.getValue());
            testJdbcConnection(storeConnectionBean);
        } else if (Store_type.HBASE == storeType) {
            log.warn("暂不支持 {} 连接测试", storeType);
        } else if (Store_type.SOLR == storeType) {
            try {
                SolrParam solrParam = new SolrParam();
                solrParam.setCollection(storeConnectionBean.getCollection());
                solrParam.setSolrZkUrl(storeConnectionBean.getSolr_zk_url());
                solrParam.setPrinciple_name(storeConnectionBean.getPrncipal_name());
                try (ISolrOperator os = SolrFactory.getSolrOperatorInstance(solrParam, null)) {
                    os.testConnectSolr();
                } catch (Exception e) {
                    throw new BusinessException("获取solr操作实例失败！ e:" + e);
                }
            } catch (Exception e) {
                throw new BusinessException(String.format("测试solr连接失败:%s", e));
            }
        } else if (Store_type.ElasticSearch == storeType) {
            log.warn("暂不支持 {} 连接测试", storeType);
        } else if (Store_type.MONGODB == storeType) {
            log.warn("暂不支持 {} 连接测试", storeType);
        } else if (Store_type.KAFKA == storeType) {
            log.warn("暂不支持 {} 连接测试", storeType);
        } else if (Store_type.CARBONDATA == storeType) {
            log.warn("暂不支持 {} 连接测试", storeType);
        } else {
            throw new BusinessException(String.format("暂不支持此数据存储类型:%s", storeType.getValue()));
        }
    }

    private static void testJdbcConnection(StoreConnectionBean storeConnectionBean) {
        JDBCBean jdbcBean = new JDBCBean();
        jdbcBean.setDatabase_type(storeConnectionBean.getDatabase_type());
        jdbcBean.setDatabase_drive(storeConnectionBean.getDatabase_driver());
        jdbcBean.setDatabase_name(storeConnectionBean.getDatabase_name());
        jdbcBean.setUser_name(storeConnectionBean.getUser_name());
        jdbcBean.setDatabase_pad(new String(Base64.getDecoder().decode(storeConnectionBean.getDatabase_pwd())));
        jdbcBean.setJdbc_url(new String(Base64.getDecoder().decode(storeConnectionBean.getJdbc_url())));
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(jdbcBean)) {
            if (!db.isConnected()) {
                throw new BusinessException("测试连接失败");
            }
        } catch (Exception e) {
            throw new BusinessException(String.format("测试连接失败：e:%s", e));
        }
    }

    private static void checkJdbcParams(StoreConnectionBean storeConnectionBean) {
        Validator.notBlank(storeConnectionBean.getDatabase_driver(), "数据库驱动信息不能为空");
        Validator.notBlank(storeConnectionBean.getDatabase_name(), "数据库名称不能为空");
        Validator.notBlank(storeConnectionBean.getUser_name(), "数据库用户名不能为空");
        Validator.notBlank(storeConnectionBean.getDatabase_pwd(), "数据库密码不能为空");
        Validator.notBlank(storeConnectionBean.getJdbc_url(), "数据库url不能为空");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file", desc = "", range = "")
    @Param(name = "database_driver", desc = "", range = "")
    public void testConnectionByDriver(MultipartFile file, StoreConnectionBean storeConnectionBean) {
        String originalFilename = file.getOriginalFilename();
        try {
            File uploadFile = new File(WebinfoProperties.FileUpload_SavedDirName + File.separator + originalFilename);
            if (!uploadFile.exists()) {
                file.transferTo(uploadFile);
            }
            if (!uploadFile.exists()) {
                throw new BusinessException(String.format("上传的驱动jar不存在:%s", uploadFile.getAbsolutePath()));
            } else {
                Store_type storeType = Store_type.ofEnumByCode(storeConnectionBean.getStore_type());
                if (Store_type.DATABASE == storeType) {
                    checkJdbcParams(storeConnectionBean);
                    testJdbcConnectionByDriver(storeConnectionBean, uploadFile);
                } else if (Store_type.HIVE == storeType) {
                    checkJdbcParams(storeConnectionBean);
                    storeConnectionBean.setDatabase_type(Store_type.HIVE.getValue());
                    testJdbcConnectionByDriver(storeConnectionBean, uploadFile);
                } else if (Store_type.HBASE == storeType) {
                    log.warn("暂不支持 {} 连接测试", storeType);
                } else if (Store_type.SOLR == storeType) {
                    log.warn("暂不支持 {} 连接测试", storeType);
                } else if (Store_type.ElasticSearch == storeType) {
                    log.warn("暂不支持 {} 连接测试", storeType);
                } else if (Store_type.MONGODB == storeType) {
                    log.warn("暂不支持 {} 连接测试", storeType);
                } else if (Store_type.KAFKA == storeType) {
                    log.warn("暂不支持 {} 连接测试", storeType);
                } else if (Store_type.CARBONDATA == storeType) {
                    log.warn("暂不支持 {} 连接测试", storeType);
                } else {
                    throw new BusinessException(String.format("暂不支持此数据存储类型:%s", storeType.getValue()));
                }
            }
        } catch (IOException e) {
            throw new BusinessException(String.format("上传驱动jar失败：%s", e));
        } catch (ClassNotFoundException e) {
            throw new BusinessException(String.format("加载驱动到classpath失败：%s", e));
        } catch (SQLException | InstantiationException | IllegalAccessException e) {
            throw new BusinessException(String.format("测试连接失败：%s", e));
        }
    }

    private static void testJdbcConnectionByDriver(StoreConnectionBean storeConnectionBean, File uploadFile) throws MalformedURLException, ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        URL[] driverUrls = new URL[] { (uploadFile.toURI().toURL()) };
        URLClassLoader urlClassLoader = new URLClassLoader(driverUrls);
        Class<?> aClass = Class.forName(storeConnectionBean.getDatabase_driver(), true, urlClassLoader);
        Driver driver = (Driver) aClass.newInstance();
        DriverImpl driverImpl = new DriverImpl(driver);
        DriverManager.registerDriver(driverImpl);
        String pwd = new String(Base64.getDecoder().decode(storeConnectionBean.getDatabase_pwd()));
        String jdbcUrl = new String(Base64.getDecoder().decode(storeConnectionBean.getJdbc_url()));
        DriverManager.getConnection(jdbcUrl, storeConnectionBean.getUser_name(), pwd);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_source", desc = "", range = "")
    @Param(name = "dsl_goal", desc = "", range = "")
    public Result searchDataStoreByDslType(String dsl_source, String dsl_goal) {
        StringBuilder sql = new StringBuilder("select * from " + DataStoreLayer.TableName);
        if (StringUtil.isNotBlank(dsl_source)) {
            sql.append(" WHERE").append(" dsl_source = '").append(dsl_source).append("'");
        }
        if (StringUtil.isNotBlank(dsl_goal)) {
            if (sql.toString().contains("WHERE")) {
                sql.append(" AND ").append(" dsl_goal = '").append(dsl_goal).append("'");
            } else {
                sql.append(" WHERE").append(" dsl_goal = '").append(dsl_goal).append("'");
            }
        }
        return Dbo.queryResult(sql.toString());
    }
}
