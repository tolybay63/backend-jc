package tofi.api.mdl.impl

import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import tofi.api.mdl.ApiMetaCube
import tofi.api.mdl.utils.dimPeriod.DimPeriodItemMdbUtils
import tofi.api.mdl.utils.tree.DataTreeNode

class ApiMetaCubeImpl extends BaseMdbUtils implements ApiMetaCube {


    @Override
    Store loadDimPeriod(long dimPeriod, String curDate) {
        DimPeriodItemMdbUtils ut = new DimPeriodItemMdbUtils(getMdb(), "DimPeriodItem.view")
        DataTreeNode dtn = ut.loadTreeNode([dimPeriod: dimPeriod, curDate: curDate])
        return dtn.getStore()
    }

    @Override
    void saveCubeInfo(Map<String, Object> params) {
        mdb.insertRec("DataCubeS", params, true)
    }
}
