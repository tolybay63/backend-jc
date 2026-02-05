package tofi.mdl.model.dao.factor;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.Map;

public class FactorRelDao extends BaseModelDao {

    public Store load(Map<String, Object> params) throws Exception {
        FactorRelMdbUtils u = new FactorRelMdbUtils(getMdb());
        return u.load(params);
    }

    public Store factor2(Map<String, Object> params) throws Exception {
        FactorRelMdbUtils u = new FactorRelMdbUtils(getMdb());
        return u.factor2(params);
    }

    public Map<String, Object> factorValRel(Map<String, Object> params) throws Exception {
        FactorRelMdbUtils u = new FactorRelMdbUtils(getMdb());
        return u.factorValRel(params);
    }

    public Store saveFactorValRel(Map<String, Object> factors, Map<String, Object> data) throws Exception {
        FactorRelMdbUtils u = new FactorRelMdbUtils(getMdb());
        return u.saveFactorValRel(factors, data);
    }

}
