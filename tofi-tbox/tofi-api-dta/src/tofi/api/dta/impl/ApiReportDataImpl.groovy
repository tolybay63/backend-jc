package tofi.api.dta.impl


import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import tofi.api.dta.ApiReportData
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiReportDataImpl extends BaseMdbUtils implements ApiReportData {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }


    @Override
    Store loadSql(String sql, String domain) {
        if (domain.isEmpty())
            return mdb.loadQuery(sql)
        else {
            Store st = mdb.createStore(domain)
            return mdb.loadQuery(st, sql)
        }
    }


}
