package tofi.mdl.model.dao.prop.multi;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.List;
import java.util.Map;

public class MultiPropDao extends BaseModelDao {

    public Store newRecMultiProp(long propGr) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.newRecMultiProp(propGr);
    }

    public Store loadMultiProp(long propGr, String lang) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadMultiProp(propGr, lang);
    }

    public void deleteMultiProp(Map<String, Object> params) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        ut.deleteMultiProp(params);
    }

    public Store insertMultiProp(Map<String, Object> params) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.insertMultiProp(params);
    }

    public Store updateMultiProp(Map<String, Object> params) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.updateMultiProp(params);
    }

    public boolean checkStatus(long multiProp) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.checkStatus(multiProp);
    }

    public boolean checkProvider(long multiProp) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.checkProvider(multiProp);
    }

    public boolean checkCondition(long multiProp) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.checkCondition(multiProp);
    }

    public Store loadStatus(long fk) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadStatus(fk);

    }

    public Store loadStatusForUpd(long prop, long factor) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadStatusForUpd(prop, factor);
    }

    public String saveStatus(long prop, List<Map<String, Object>> params) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.saveStatus(prop, params);
    }

    public Store loadProvider(long prop) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadProvider(prop);
    }

    public void deleteProvider(long id) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        ut.deleteProvider(id);
    }

    //todo Delete!
    public Store loadProviderForUpd(long prop, long typ) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadProviderForUpd(prop, typ);
    }

    public Store loadProviderClsForSelect(long prop, long typ, String mode) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadProviderClsForSelect(prop, typ, mode);
    }


    public StoreRecord saveProvider(Map<String, Object> rec, String mode) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.saveProvider(rec, mode);
    }

    public Store loadRec(long id, String lang) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadRec(id, lang);
    }

    public Store loadMultiPropDim(long pm) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadMultiPropDim(pm);
    }

    public Store loadMultiPropDimForUpd(long multiProp) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadMultiPropDimForUpd(multiProp);
    }

    public void saveMultiPropDim(long multiProp, List<Map<String, Object>> params) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        ut.saveMultiPropDim(multiProp, params);
    }

    public void changeOrdMultiPropDim(Map<String, Object> params) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        ut.changeOrdMultiPropDim(params);
    }

    public Store loadMultiPropPeriodType(long multiProp) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadMultiPropPeriodType(multiProp);
    }

    public Store loadMultiPropPeriodTypeForUpd(long multiProp) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadMultiPropPeriodTypeForUpd(multiProp);
    }

    public String saveMultiPropPeriodType(long multiProp, List<Map<String, Object>> params) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.saveMultiPropPeriodType(multiProp, params);
    }

    //MultiPropCond
    public Store loadMultiPropCond(long multiProp) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadMultiPropCond(multiProp);
    }

    public void deleteMultiPropCond(long id) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        ut.deleteMultiPropCond(id);
    }

    public Store loadFactorsForSelect(long multiProp, String mode) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadFactorsForSelect(multiProp, mode);
    }

    public Store loadTypsForSelect(long multiProp, String mode) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadTypsForSelect(multiProp, mode);
    }

    public Store loadRelTypsForSelect(long multiProp, String mode) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        return ut.loadRelTypsForSelect(multiProp, mode);
    }

    public void insertMultiPropCond(Map<String, Object> rec) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        ut.insertMultiPropCond(rec);
    }

    public void updateMultiPropCond(Map<String, Object> rec) throws Exception {
        MultiPropMdbUtils ut = new MultiPropMdbUtils(getMdb(), "MultiProp");
        ut.updateMultiPropCond(rec);
    }

}
