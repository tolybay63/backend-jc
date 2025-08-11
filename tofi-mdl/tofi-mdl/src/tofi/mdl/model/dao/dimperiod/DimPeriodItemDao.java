package tofi.mdl.model.dao.dimperiod;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import tofi.mdl.model.utils.tree.DataTreeNode;

import java.util.Map;

public class DimPeriodItemDao extends BaseModelDao {

    public Store load(Map<String, Object> params) throws Exception {
        DimPeriodItemMdbUtils ut = new DimPeriodItemMdbUtils(getMdb(), "DimPeriodItem.view");
        return ut.load(params);
    }

    public Store loadTree(Map<String, Object> params) throws Exception {
        DimPeriodItemMdbUtils ut = new DimPeriodItemMdbUtils(getMdb(), "DimPeriodItem.view");
        DataTreeNode dtn = ut.loadTreeNode(params);
        return dtn.getStore();
    }


}
