package tofi.api.dta;

import jandcode.core.store.Store;

public interface ApiReportData {

    /**
     * @param sql text of Sql
     * @return Store
     */
    Store loadSql(String sql, String domain);



}
