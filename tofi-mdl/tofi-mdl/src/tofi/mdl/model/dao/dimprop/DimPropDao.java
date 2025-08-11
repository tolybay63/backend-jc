package tofi.mdl.model.dao.dimprop;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class DimPropDao extends BaseModelDao {

    public Store loadDimProp(long dimPropGr) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimProp");
        return ut.loadDimProp(dimPropGr);
    }

    public StoreRecord newRec(long dimPropGroup) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimProp");
        return ut.newRec(dimPropGroup);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimProp");
        return ut.insert(params);
    }

    public Store update(Map<String, Object> params) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimProp");
        return ut.update(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimProp");
        ut.delete(params);
    }

    //???
    public Store loadDimPropItemPropForUpd(long dimprop) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadDimPropItemPropForUpd(dimprop);
    }
    //???

    public Store loadDimPropItemProp(long dimprop, long dimpropType) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadDimPropItemProp(dimprop, dimpropType);
    }

    public Store insertDPI(Map<String, Object> rec) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.insertDPI(rec);
    }

    public Store updateDPI(Map<String, Object> rec) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.updateDPI(rec);
    }

    public void deleteDPI(long id) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        ut.deleteDPI(id);
    }

    public Store loadForFvSelect(long dimPropItem) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadForFvSelect(dimPropItem);
    }

    public Store loadAllDimMultiPropForSelect() throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadAllDimMultiPropForSelect();
    }

    public Store loadAllMultiPropForSelect() throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadAllMultiPropForSelect();
    }

    public Map<String, Long> getDimMultiPropType(long dimProp) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.getDimMultiPropType(dimProp);
    }

    public Store loadAllDimMultiPropItemForSelect() throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadAllDimMultiPropItemForSelect();
    }

    public Store loadDimMultiPropItemForSelect(long dimMultiProp) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadDimMultiPropItemForSelect(dimMultiProp);
    }

/*
    public Store loadStatusForSelect() throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadStatusForSelect();
    }

    public Store loadProviderForSelect(long dimProp, String mode) throws Exception {
        DimPropMdbUtils ut = new DimPropMdbUtils(getMdb(), "DimPropItem");
        return ut.loadProviderForSelect(dimProp, mode);
    }
*/
}
