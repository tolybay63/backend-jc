package tofi.mdl.model.dao.meter;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class MeterFactorDao extends BaseModelDao {
    public Store load(Map<String, Object> params) throws Exception {
        MeterFactorMdbUtils mdbUtils = new MeterFactorMdbUtils(getMdb());
        return mdbUtils.load(params);
    }

    public StoreRecord newRec(Map<String, Object> params) throws Exception {
        MeterFactorMdbUtils mdbUtils = new MeterFactorMdbUtils(getMdb());
        return mdbUtils.newRec(params);
    }

    public void insert(Map<String, Object> params) throws Exception {
        MeterFactorMdbUtils mdbUtils = new MeterFactorMdbUtils(getMdb());
        mdbUtils.insert(params);
    }

    public void delFactor(Map<String, Object> params) throws Exception {
        MeterFactorMdbUtils mdbUtils = new MeterFactorMdbUtils(getMdb());
        mdbUtils.delFactor(params);
    }

    public Store factors(Map<String, Object> params) throws Exception {
        MeterFactorMdbUtils mdbUtils = new MeterFactorMdbUtils(getMdb());
        return mdbUtils.factors(params);
    }

    public void changeOrd(Map<String, Object> params) throws Exception {
        MeterFactorMdbUtils mdbUtils = new MeterFactorMdbUtils(getMdb());
        mdbUtils.changeOrd(params);
    }

    public void newDimension(Map<String, Object> params) throws Exception {
        MeterFactorMdbUtils mdbUtils = new MeterFactorMdbUtils(getMdb());
        mdbUtils.newDimension(params);
    }
}
