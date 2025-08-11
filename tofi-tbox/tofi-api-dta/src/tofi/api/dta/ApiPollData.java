package tofi.api.dta;

import jandcode.core.store.Store;

import java.util.Map;

public interface ApiPollData {

    /**
     *
     * @param sql text of Sql
     * @return Store
     */
    Store loadSql(String sql, String domain);

    /**
     *
     * @param sql text sql
     * @param params params Map
     * @param domain domain
     * @return Store
     */
    Store loadSqlWithParams(String sql, Map<String, Object> params, String domain);

    /**
     *
     * @param tableName name of table
     * @param params params
     * @return id of table
     */
    long insertRecToTable(String tableName, Map<String, Object> params, boolean generateId);

    /**
     *
     * @param sql text sql
     */
    void execSql(String sql);

    /**
     *
     * @param tableName name of table
     * @param params params Map
     */
    void updateTable(String tableName, Map<String, Object> params);

    /**
     *
     * @param tableName name of table
     * @param id id of table
     */
    void deleteTable(String tableName, long id);

    /**
     * create Owner
     * @param params params
     */
    long createOwner(Map<String, Object> params);


}
