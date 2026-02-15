package tofi.mdl.model.dao.meter;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class MeterDao extends BaseModelDao {

    public Map<String, Object> loadMeterPaginate(Map<String, Object> params) throws Exception {
        MeterMdbUtils u = new MeterMdbUtils(getMdb(), "Meter");
        return u.loadMeterPaginate(params);
    }


    public void delete(Map<String, Object> params) throws Exception {
        MeterMdbUtils u = new MeterMdbUtils(getMdb(), "Meter");
        u.delete(UtCnv.toMap(params.get("rec")));
    }

    public Store update(Map<String, Object> params) throws Exception {
        MeterMdbUtils u = new MeterMdbUtils(getMdb(), "Meter");
        return u.update(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        MeterMdbUtils u = new MeterMdbUtils(getMdb(), "Meter");
        return u.insert(params);
    }

    public Store loadRec(Map<String, Object> params) throws Exception {
        MeterMdbUtils u = new MeterMdbUtils(getMdb(), "Meter");
        return u.loadRec(params);
    }

    public StoreRecord newRec(Map<String, Object> params) throws Exception {
        MeterMdbUtils u = new MeterMdbUtils(getMdb(), "Meter");
        return u.newRec(params);
    }

    public Store loadForSelect(String lang) throws Exception {
        MeterMdbUtils u = new MeterMdbUtils(getMdb(), "Meter");
        return u.loadForSelect(lang);
    }

}
