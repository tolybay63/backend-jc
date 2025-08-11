package tofi.mdl.model.dao.dimobj;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class DimObjDao extends BaseModelDao {

    public Store loadDimObj(long dimObjGr) throws Exception {
        DimObjMdbUtils ut = new DimObjMdbUtils(getMdb(), "DimObj");
        return ut.loadDimObj(dimObjGr);
    }

    public StoreRecord newRec(long dimObjGroup) throws Exception {
        DimObjMdbUtils ut = new DimObjMdbUtils(getMdb(), "DimObj");
        return ut.newRec(dimObjGroup);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        DimObjMdbUtils ut = new DimObjMdbUtils(getMdb(), "DimObj");
        return ut.insert(params);
    }

    public Store update(Map<String, Object> params) throws Exception {
        DimObjMdbUtils ut = new DimObjMdbUtils(getMdb(), "DimObj");
        return ut.update(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        DimObjMdbUtils ut = new DimObjMdbUtils(getMdb(), "DimObj");
        ut.delete(params);
    }
    //********************************* DimObjItem *********************************//

    public Store loadDimObjItem(long dimobj) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadDimObjItem(dimobj);
    }

    public Store loadTypForSelect(long relTyp, long parent, long doit, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadTypForSelect(relTyp, parent, doit, linkType);
    }

    public Store loadClsForSelect(long typ) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadClsForSelect(typ);
    }

    public Store loadRelTypMember(long memberType, long relTyp,
                                  long parent, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadRelTypMember(memberType, relTyp, parent, linkType);
    }

    public Store loadRelClsMember(long memberType, long relCls, long parent, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadRelClsMember(memberType, relCls, parent, linkType);
    }



/*    public Store loadObjForSelect(long cls) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadObjForSelect(cls);
    }*/

    public Store loadRelTypForSelect(long doit, long parent, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadRelTypForSelect(doit, parent, linkType);
    }

    public Store loadRelClsForSelect(long relTyp, long doit, long parent, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadRelClsForSelect(relTyp, doit, parent, linkType);
    }

    public StoreRecord insertDOI(Map<String, Object> params) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.insertDOI(params);
    }

    public StoreRecord updateDOI(Map<String, Object> params) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.updateDOI(params);
    }

    public void deleteDOI(long id) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        ut.deleteDOI(id);
    }

    //--------------------------------------------------

    public Store loadDimObjItemProp(long dimObjItem) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadDimObjItemProp(dimObjItem);
    }

    public Store loadPropStatus(long doi, long prop, String mode) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadPropStatus(doi, prop, mode);
    }

    public Store loadPropProvider(long prop) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadPropProvider(prop);
    }

    public Store loadOptProp(long doi) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadOptProp(doi);
    }

    public Store loadOptPropVal(long prop, long pt, long doip, String mode) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadOptPropVal(prop, pt, doip, mode);
    }

    public Store insertDOIprop(Map<String, Object> params) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.insertDOIprop(params);
    }

    public Store updateDOIprop(Map<String, Object> params) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.updateDOIprop(params);
    }

    public void deleteDOIprop(long id) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        ut.deleteDOIprop(id);
    }

    public void insertDOIpropValue(Map<String, Object> rec) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        ut.insertDOIpropValue(rec);
    }

    public void updateDOIpropValue(Map<String, Object> rec) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        ut.updateDOIpropValue(rec);
    }

    public void deleteDOIpropValue(long id) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        ut.deleteDOIpropValue(id);
    }

    public Store loadTreeForView(Map<String, Object> params) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadTreeForView(params);
    }

    public Store loadDimObjItemPropVal(long dimObjItemProp, long prop, long pt) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadDimObjItemPropVal(dimObjItemProp, prop, pt);
    }

    public Store loadOptForRefValues(String ent) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadOptForRefValues(ent);
    }

    public Store loadProp(long doit, long parent, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadProp(doit, parent, linkType);
    }

    public Store loadValueOfProp(long doit, long prop, long parent, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadValueOfProp(doit, prop, parent, linkType);
    }

    public Store loadProp_6(long memberType, long typORrel, long doit, long parent) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadProp_6(memberType, typORrel, doit, parent);
    }

    public Store loadValueOfProp_7(long doit, long prop, long rtmORrcm, long typORrel, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadValueOfProp_7(doit, prop, rtmORrcm, typORrel, linkType);
    }

    public Store loadProp_10_11(long doit, long rtmORrcm, long parent, long linkType) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadProp_10_11(doit, rtmORrcm, parent, linkType);
    }

    public Store loadCompFor_11(long doit, long prop) throws Exception {
        DimObjItemMdbUtils ut = new DimObjItemMdbUtils(getMdb());
        return ut.loadCompFor_11(doit, prop);
    }

}
