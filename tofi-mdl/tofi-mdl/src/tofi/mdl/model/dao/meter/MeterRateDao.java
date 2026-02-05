package tofi.mdl.model.dao.meter;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.Map;

public class MeterRateDao extends BaseModelDao {

    public Store loadSoftMR(long meter, String lang) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        return mdbUtils.loadSoftMR(meter, lang);
    }

    public void createAllSoftMR(long meter) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        mdbUtils.createAllSoftMR(meter);
    }

    public void updateSoftMR(Map<String, Object> params) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        mdbUtils.updateSoftMR(params);
    }

    public void deleteSoftMR(Map<String, Object> params) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        mdbUtils.deleteSoftMR(params);
    }

    public void deleteAllMR(long meter) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        mdbUtils.deleteAllMR(meter);
    }

    //-------------------------------
    public Store loadHardMR(long meter, String lang) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        return mdbUtils.loadHardMR(meter, lang);
    }

    public void insertHardMR(Map<String, Object> params) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        mdbUtils.insertHardMR(params);
    }

    public void updateHardMR(Map<String, Object> params) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        mdbUtils.updateHardMR(params);
    }

    public void deleteHardMR(Map<String, Object> params) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        mdbUtils.deleteHardMR(params);
    }

    public Store loadMeterSoftForUpd(long meter, String lang) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        return mdbUtils.loadMeterSoftForUpd(meter, lang);
    }

    public Store loadMeterSoftForUpdSave(Map<String, Object> params) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        return mdbUtils.loadMeterSoftForUpdSave(params);
    }

    public void saveMeterSoftRates(Map<String, Object> params) throws Exception {
        MeterRateMdbUtils mdbUtils = new MeterRateMdbUtils(getMdb(), "MeterRate");
        mdbUtils.saveMeterSoftRates(params);
    }
}
