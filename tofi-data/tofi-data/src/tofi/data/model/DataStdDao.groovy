package tofi.data.model

import jandcode.core.dbm.mdb.BaseMdbUtils
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class DataStdDao extends BaseMdbUtils {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    ApinatorApi apiUserData() {
        return app.bean(ApinatorService).getApi("userdata")
    }

    ApinatorApi apiPollData() {
        return app.bean(ApinatorService).getApi("polldata")
    }
    //






}
