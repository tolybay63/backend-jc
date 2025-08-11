package tofi.mdl.model.dao.measure;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.Map;

public class MeasureDao extends BaseModelDao {

    public Store load(Map<String, Object> params) throws Exception {
        MeasureMdbUtils ut = new MeasureMdbUtils(getMdb(), "Measure");
        return ut.load(params);
    }

    public Store loadBase(Map<String, Object> params) throws Exception {
        MeasureMdbUtils ut = new MeasureMdbUtils(getMdb(), "Measure");
        return ut.loadBase(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        MeasureMdbUtils ut = new MeasureMdbUtils(getMdb(), "Measure");
        return ut.insert(params);
    }

    public Store update(Map<String, Object> params) throws Exception {
        MeasureMdbUtils ut = new MeasureMdbUtils(getMdb(), "Measure");
        return ut.update(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        MeasureMdbUtils u = new MeasureMdbUtils(getMdb(), "Measure");
        u.delete(UtCnv.toMap(params.get("rec")));
    }

}
