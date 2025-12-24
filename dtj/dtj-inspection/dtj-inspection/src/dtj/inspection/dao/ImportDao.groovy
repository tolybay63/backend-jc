package dtj.inspection.dao

import jandcode.core.dao.DaoMethod
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ImportDao {
    ApinatorApi apiMeta() { return app.bean(ApinatorService).getApi("meta") }

    @DaoMethod
    void analize() {


    }



}
