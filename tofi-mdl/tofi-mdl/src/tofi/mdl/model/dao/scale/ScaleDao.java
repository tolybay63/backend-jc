package tofi.mdl.model.dao.scale;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.List;
import java.util.Map;

public class ScaleDao extends BaseModelDao {

    public Store load(Map<String, Object> params) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "Scale");
        return ut.load(params);
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "Scale");
        return ut.insert(rec);
    }

    public Store update(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "Scale");
        return ut.update(rec);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "Scale");
        ut.delete(rec);
    }

    //ScalVal
    public Store loadScaleVal(long scale) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleVal");
        return ut.loadScaleVal(scale);
    }

    public Store insertScaleVal(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleVal");
        return ut.insertScaleVal(rec);
    }

    public Store updateScaleVal(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleVal");
        return ut.updateScaleVal(rec);
    }

    public void deleteScaleVal(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleVal");
        ut.deleteScaleVal(rec);
    }


    //ScalAsgn
    public Store loadScaleAsgn(long scale) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleAsgn");
        return ut.loadScaleAsgn(scale);
    }

    public Store insertScaleAsgn(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleAsgn");
        return ut.insertScaleAsgn(rec);

    }

    public Store updateScaleAsgn(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleAsgn");
        return ut.updateScaleAsgn(rec);
    }

    public void deleteScaleAsgn(Map<String, Object> rec) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleAsgn");
        ut.deleteScaleAsgn(rec);
    }

    //ScaleValAsgn
    public Store loadScaleValAsgn(long scale, long scaleAsgn) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleValAsgn");
        return ut.loadScaleValAsgn(scale, scaleAsgn);
    }

    public Store loadScaleValAsgnUpd(long scale, long scaleAsgn, String nmAsgn) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleValAsgn");
        return ut.loadScaleValAsgnUpd(scale, scaleAsgn, nmAsgn);
    }

    public void updateScaleValAsgn(long scale, long scaleAsgn, List<Map<String, Object>> data) throws Exception {
        ScaleMdbUtils ut = new ScaleMdbUtils(getMdb(), "ScaleValAsgn");
        ut.updateScaleValAsgn(scale, scaleAsgn, data);
    }
}
