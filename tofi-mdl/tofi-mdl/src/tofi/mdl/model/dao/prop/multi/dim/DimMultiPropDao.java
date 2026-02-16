package tofi.mdl.model.dao.prop.multi.dim;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.Map;

public class DimMultiPropDao extends BaseModelDao {

    public Store newRec(long propGr) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        return u.newRec(propGr);
    }

    public Store loadDimMultiProp(long propGr, String lang) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        return u.loadDimMultiProp(propGr, lang);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        return u.insert(params);
    }

    public Store update(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        return u.update(params);
    }

    public void delete(Map<String, Object> parems) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        u.delete(UtCnv.toMap(parems.get("rec")));
    }

    //DimMultiPropItem
    public Map<String, Object> loadDimMultiPropItem(long propDim) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadDimMultiPropItem(propDim);
    }

    public Store newRecDimMultiPropItem(long propDim) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.newRecDimMultiPropItem(propDim);
    }

    public Store insertDimItem(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.insertDimItem(params);
    }

    public Store updateDimItem(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.updateDimItem(params);
    }

    public void deleteDimItem(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        u.deleteDimItem(params);
    }


    ///*****************************************************************************

    public Store loadClsForSelect(String lang) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadClsForSelect(lang);
    }

    public Store loadObjForSelect(long cls) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadObjForSelect(cls);
    }

    public Store loadRelClsForSelect(String lang) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadRelClsForSelect(lang);
    }

    public Store loadRelObjForSelect(long relCls) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadRelObjForSelect(relCls);
    }

    public Store loadProp() throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadProp();
    }

    public Store loadDimMultiPropItemAttrib(long dimMultiPropItem) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadDimMultiPropItemAttrib(dimMultiPropItem);
    }

    public Store insDimMultiPropItemAttrib(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.insDimMultiPropItemAttrib(params);
    }

    public Store updDimMultiPropItemAttrib(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.updDimMultiPropItemAttrib(params);
    }

    public void deleteDimMultiPropItemAttrib(long id) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        u.deleteDimMultiPropItemAttrib(id);
    }
    //
    public Store loadDimMultiPropItemMeter(long dimMultiPropItem) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadDimMultiPropItemMeter(dimMultiPropItem);
    }

    public Store insDimMultiPropItemMeter(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.insDimMultiPropItemMeter(params);
    }

    public Store updDimMultiPropItemMeter(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.updDimMultiPropItemMeter(params);
    }

    public void deleteDimMultiPropItemMeter(long id) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        u.deleteDimMultiPropItemMeter(id);
    }

    public Store loadMeasure() throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadMeasure();
    }

    public Store loadDimMultiPropItemFactor(long dimMultiPropItem) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadDimMultiPropItemFactor(dimMultiPropItem);
    }

    public Store insDimMultiPropItemFactor(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.insDimMultiPropItemFactor(params);
    }

    public Store updDimMultiPropItemFactor(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.updDimMultiPropItemFactor(params);
    }

    public void deleteDimMultiPropItemFactor(long id) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        u.deleteDimMultiPropItemFactor(id);
    }

    public Store loadFactors() throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadFactors();
    }

    //
    public Store loadDimMultiPropItemCls(long dimMultiPropItem) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadDimMultiPropItemCls(dimMultiPropItem);
    }

    public Store insDimMultiPropItemCls(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.insDimMultiPropItemCls(params);
    }

    public Store updDimMultiPropItemCls(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.updDimMultiPropItemCls(params);
    }

    public void deleteDimMultiPropItemCls(long id) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        u.deleteDimMultiPropItemCls(id);
    }

    //RelCls
    public Store loadDimMultiPropItemRelCls(long dimMultiPropItem) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadDimMultiPropItemRelCls(dimMultiPropItem);
    }

    public Store insDimMultiPropItemRelCls(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.insDimMultiPropItemRelCls(params);
    }

    public Store updDimMultiPropItemRelCls(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.updDimMultiPropItemRelCls(params);
    }

    public void deleteDimMultiPropItemRelCls(long id) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        u.deleteDimMultiPropItemRelCls(id);
    }

    //Measure
    public Store loadDimMultiPropItemMeasure(long dimMultiPropItem) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadDimMultiPropItemMeasure(dimMultiPropItem);
    }

    public Store insDimMultiPropItemMeasure(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.insDimMultiPropItemMeasure(params);
    }

    public Store updDimMultiPropItemMeasure(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.updDimMultiPropItemMeasure(params);
    }

    public void deleteDimMultiPropItemMeasure(long id) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        u.deleteDimMultiPropItemMeasure(id);
    }

    public Store loadDimMultiPropMoreCols(long dimMultiProp) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        return u.loadDimMultiPropMoreCols(dimMultiProp);
    }

    public Store insertMoreCols(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        return u.insertMoreCols(params);
    }

    public Store updateMoreCols(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        return u.updateMoreCols(params);
    }

    public void deleteMoreCols(long id) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        u.deleteMoreCols(id);
    }

    public void changeOrdMoreCols(Map<String, Object> params) throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiProp");
        u.changeOrdMoreCols(params);
    }

    public Store loadPropForMultiPropItem() throws Exception {
        DimMultiPropMdbUtils u = new DimMultiPropMdbUtils(getMdb(), "DimMultiPropItem");
        return u.loadPropForMultiPropItem();
    }

}
