package tofi.api.dta;

import jandcode.core.store.Store;

import java.util.Map;

public interface ApiNSIData {

    /**
     * @param sql text of Sql
     * @return Store
     */
    Store loadSql(String sql, String domain);

    /**
     * @param sql    text sql
     * @param params params Map
     * @param domain domain
     * @return Store
     */
    Store loadSqlWithParams(String sql, Map<String, Object> params, String domain);

    /**
     * @param tableName name of table
     * @param params    params
     * @return id of table
     */
    long insertRecToTable(String tableName, Map<String, Object> params, boolean generateId);

    /**
     * @param sql text sql
     */
    void execSql(String sql);

    /**
     * @param tableName name of table
     * @param params    params Map
     */
    void updateTable(String tableName, Map<String, Object> params);

    long insertTable(String tableName, Map<String, Object> params);

    /**
     * @param tableName name of table
     * @param id        id of table
     */
    void deleteTable(String tableName, long id);

    /**
     * create Owner
     *
     * @param params params
     */
    long createOwner(Map<String, Object> params);

    /**
     * @param rec Props of File
     */
    void attachFile(Map<String, Object> rec);

    /**
     *
     * @param codCls cod of Cls
     * @param codProp cod of Prop
     * @return store Obj for select
     */
    Store loadObjForSelect(String codCls, String codProp);

    /**
     *
     * @param codCls cod of Cls
     * @param codProp cod of Prop
     * @return store Obj for select
     */
    Store loadObjTreeForSelect(String codCls, String codProp);

    /**
     *
     * @param owner id Obj or RelObj
     * @param isObj boolean
     * @return id Cls or id RelCls
     */
    long getClsOrRelCls(long owner, int isObj);

    /**
     *
     * @param entId id Entity (factorval, obj, relobj, measure
     * @param entName name of Entity
     * @param propVal id of PropVal
     * @return boolean
     */
    boolean is_exist_entity_as_data(long entId, String entName, String propVal);

    boolean is_exist_entity_as_dataOld(long entId, String entName, long propVal);

    /**
     *
     * @param clsORrelcls id Cls or id RelCls
     * @param isObj 1=>Obj, 0=>RelObj
     */
    boolean checkExistOwners(long clsORrelcls, boolean isObj);

    void deleteEntity(long entId, String tableName);

}
