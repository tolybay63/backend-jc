package tofi.mdl.model.dao.factor;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class FactorDao extends BaseModelDao {

    public Map<String, Object> loadFactorPaginate(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.loadFactorPaginate(params);
    }

    public Store loadFactorVal(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.loadFactorVal(params);
    }

    public Store loadFactorTree(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.loadFactorTree(params);
    }

    public Store loadForSelect() throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.loadForSelect();
    }

    public void delete(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        u.delete(UtCnv.toMap(params.get("rec")));
    }

    public Store update(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.update(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.insert(params);
    }

    public StoreRecord loadRec(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.loadRec(params);
    }

    public void changeOrdFV(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        u.changeOrdFV(params);
    }

    public void changeOrdF(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        u.changeOrdF(params);
    }

    public Store getFactor(long fv) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.getFactor(fv);
    }

    public Store loadFvForSelect(Map<String, Object> params) throws Exception {
        FactorMdbUtils u = new FactorMdbUtils(getMdb(), "Factor");
        return u.loadFvForSelect(params);
    }
}
