package tofi.api.dta;

import jandcode.core.store.Store;

import java.util.Map;

public interface ApiRepairData {

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

    void checkStatusOfIncident(long objWorkPlan, String codStatusFrom, String codStatusTo);
}
