package tofi.mdl.model.dao.dimperiod;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.List;
import java.util.Map;

public class DimPeriodDao extends BaseModelDao {

    public Map<String, Object> loadDimPeriodPaginate(Map<String, Object> params) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriod");
        return ut.loadDimPeriodPaginate(params);
    }

    public StoreRecord newRec(Map<String, Object> params) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriod");
        return ut.newRec(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriod");
        return ut.insert(params);
    }

    public Store update(Map<String, Object> params) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriod");
        return ut.update(params);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriod");
        ut.delete(rec);
    }

    ////////////////////////////////
    public Store loadDPI(long dimperiod) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        return ut.loadDPI(dimperiod);
    }

    public Store newRecDPI(boolean isChild, Map<String, Object> rec) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        return ut.newRecDPI(isChild, rec);
    }

    public String getPeriodBeg(String dbeg, long periodType) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        return ut.getPeriodBeg(dbeg, periodType);
    }

    public String getPeriodEnd(String dend, long periodType) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        return ut.getPeriodEnd(dend, periodType);
    }

    public Store insertDPI(Map<String, Object> params) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        return ut.insertDPI(params);
    }

    public Store updateDPI(Map<String, Object> params) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        return ut.updateDPI(params);
    }

    public void deleteDPI(Map<String, Object> rec) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        ut.deleteDPI(rec);
    }

    public Store loadNotIn(long dimPeriodItem) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        return ut.loadNotIn(dimPeriodItem);
    }

    public Store loadNotInForUpdate(long dimPeriodItem) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        return ut.loadNotInForUpdate(dimPeriodItem);
    }

    public void updateNotIn(long dimPeriodItem, List<Map<String, Object>> data) throws Exception {
        DimPeriodMdbUtils ut = new DimPeriodMdbUtils(getMdb(), "DimPeriodItem");
        ut.updateNotIn(dimPeriodItem, data);
    }
}
