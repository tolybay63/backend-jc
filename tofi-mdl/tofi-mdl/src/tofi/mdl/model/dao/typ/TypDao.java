package tofi.mdl.model.dao.typ;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.dao.reltyp.RelTypMemberMdbUtils;

import java.util.Map;

public class TypDao extends BaseModelDao {

    //---------------------- Typ --------------------------------
    public Map<String, Object> loadTypPaginate(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.loadTypPaginate(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        u.delete(UtCnv.toMap(params.get("rec")));
    }

    public Store update(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.update(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.insert(params);
    }

    public Store loadRec(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.loadRec(params);
    }

    public Store loadTypParent(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.loadTypParent(params);
    }


    //---------------------- TypVer --------------------------------
    public Store loadVer(long typ, String lang) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.loadVer(typ, lang);
    }

    public Store updateVer(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.updateVer(params);
    }

    public Store insertVer(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.insertVer(params);
    }

    public void deleteVer(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        u.deleteVer(UtCnv.toMap(params.get("rec")));
    }

    //---------------------- TypRole --------------------------------
    public Store loadTypRole(long id, long typ, String lang) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        return u.loadTypRole(id, typ, lang);
    }

    public Store selectTypRole(long typ, String lang) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        return u.selectTypRole(typ, lang);
    }


    public Store insertTypRole(Map<String, Object> params) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        return u.insertTypRole(params);
    }

    public Store updateTypRole(Map<String, Object> params) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        return u.updateTypRole(params);
    }

    public void deleteTypRole(Map<String, Object> params) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        u.deleteTypRole(UtCnv.toMap(params.get("rec")));
    }

    //---------------------- TypRoleLifeInterval --------------------------------
    public Store loadTypRoleLife(long id, long typRole, String lang) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        return u.loadTypRoleLife(id, typRole, lang);
    }

    public Store insertTypRoleLife(Map<String, Object> params) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        return u.insertTypRoleLife(params);
    }

    public Store updateTypRoleLife(Map<String, Object> params) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        return u.updateTypRoleLife(params);
    }

    public void deleteTypRoleLife(Map<String, Object> params) throws Exception {
        TypRoleMdbUtils u = new TypRoleMdbUtils(getMdb());
        u.deleteTypRoleLife(UtCnv.toMap(params.get("rec")));
    }

    //---------------------- TypClusterFactor --------------------------------

    public Store loadTypClusterFactor(long id, long typ, String lang) throws Exception {
        TypClusterFactorMdbUtils u = new TypClusterFactorMdbUtils(getMdb());
        return u.loadTypClusterFactor(id, typ, lang);
    }

    public Store insertTypClusterFactor(Map<String, Object> params) throws Exception {
        TypClusterFactorMdbUtils u = new TypClusterFactorMdbUtils(getMdb());
        return u.insertTypClusterFactor(params);
    }

    public Store updateTypClusterFactor(Map<String, Object> params) throws Exception {
        TypClusterFactorMdbUtils u = new TypClusterFactorMdbUtils(getMdb());
        return u.updateTypClusterFactor(params);
    }

    public void deleteTypClusterFactor(Map<String, Object> params) throws Exception {
        TypClusterFactorMdbUtils u = new TypClusterFactorMdbUtils(getMdb());
        u.deleteTypClusterFactor(UtCnv.toMap(params.get("rec")));
    }

    public Store loadFactors(long typ, String mode, String lang) throws Exception {
        TypClusterFactorMdbUtils u = new TypClusterFactorMdbUtils(getMdb());
        return u.loadFactors(typ, mode, lang);
    }

    //---------------------- Cls --------------------------------

    public Store loadCls(long typ, String lang) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.loadCls(typ, lang);
    }

    public Store loadClsFVforUpd(long typ, long cls, String lang) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "ClsFactorVal");
        return u.loadClsFVforUpd(typ, cls, lang);
    }

    public Store loadClsFV(long typ, long cls, String lang) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "ClsFactorVal");
        return u.loadClsFV(typ, cls, lang);
    }

    public Store insertCls(Map<String, Object> params) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.insertCls(params);
    }

    public Store updateCls(Map<String, Object> params) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.updateCls(params);
    }

    public void deleteCls(Map<String, Object> params) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        u.deleteCls(params);
    }

    public Store loadClsTree(Map<String, Object> params) throws Exception {
        ClsTreeMdbUtils u = new ClsTreeMdbUtils(getMdb());
        return u.loadClsTree(params);
    }

    public Store loadClsVer(long cls, String lang) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.loadClsVer(cls, lang);
    }

    public Store loadRecCls(long id, String lang) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.loadRecCls(id, lang);
    }

    public Store updateClsVer(Map<String, Object> rec) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.updateClsVer(rec);
    }

    public Store insertClsVer(Map<String, Object> rec) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.insertClsVer(rec);
    }

    public void deleteClsVer(Map<String, Object> rec) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        u.deleteClsVer(rec);
    }


    //---------------------- NotExtended --------------------------------

    public Store loadObjTreeSelect(long cls, String lang) throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.loadObjTreeSelect(cls, lang);
    }

    public Store loadNotExtended(long id, long typ, String lang) throws Exception {
        NotExtendedMdbUtils u = new NotExtendedMdbUtils(getMdb());
        return u.loadNotExtended(id, typ, lang);
    }


    public Store insertNotExtended(Map<String, Object> params) throws Exception {
        NotExtendedMdbUtils u = new NotExtendedMdbUtils(getMdb());
        return u.insertNotExtended(params);
    }

    public Store updateNotExtended(Map<String, Object> params) throws Exception {
        NotExtendedMdbUtils u = new NotExtendedMdbUtils(getMdb());
        return u.updateNotExtended(UtCnv.toMap(params.get("rec")));
    }


    public void deleteNotExtended(Map<String, Object> params) throws Exception {
        NotExtendedMdbUtils u = new NotExtendedMdbUtils(getMdb());
        u.deleteNotExtended(params);
    }


    //todo 07_09
    public Store loadClsTreeForSelect() throws Exception {
        ClsMdbUtils u = new ClsMdbUtils(getMdb(), "Cls");
        return u.loadClsTreeForSelect();
    }

    //TypCharGr
    public Store loadTypCharGr(long id, String lang) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.loadTypCharGr(id, lang);
    }

    public Store loadTypCharGrInfo(long id, String lang) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.loadTypCharGrInfo(id, lang);
    }


    //todo Delete
/*    public Store loadTypCharGr2(long typ) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.loadTypCharGr2(typ);
    }*/


    public Store loadTypClustFactorVal(long typ, String mode, String lang) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "Typ");
        return u.loadTypClustFactorVal(typ, mode, lang);
    }

    public Store insertTypCharGr(Map<String, Object> rec) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.insertTypCharGr(rec);
    }

    public Store updateTypCharGr(Map<String, Object> rec) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.updateTypCharGr(rec);
    }

    public void deleteTypCharGr(Map<String, Object> rec) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        u.deleteTypCharGr(rec);
    }

    //TypCharGrProp
    public Store loadTypCharGrProp(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.loadTypCharGrProp(params);
    }

    public Store loadTypCharGrPropForUpd(long typCharGr, String lang) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.loadTypCharGrPropForUpd(typCharGr, lang);
    }

    public String saveTypCharGrProps(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.saveTypCharGrProps(params);
    }

    //TypCharGrMultiProp
    public Store loadTypCharGrMultiProp(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.loadTypCharGrMultiProp(params);
    }

    public Store loadTypCharGrMultiPropForUpd(long typCharGr) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.loadTypCharGrMultiPropForUpd(typCharGr);
    }

    public String saveTypCharGrMultiProp(Map<String, Object> params) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.saveTypCharGrMultiProp(params);
    }

    public void updateTypCharGrMultiProp(Map<String, Object> rec) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        u.updateTypCharGrMultiProp(rec);
    }

    public void updateTypCharGrProp(Map<String, Object> rec) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        u.updateTypCharGrProp(rec);
    }

    public Store loadPropMeasure(long typCharGr) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.loadPropMeasure(typCharGr);
    }

    public Store loadMeasure(long prop) throws Exception {
        TypMdbUtils u = new TypMdbUtils(getMdb(), "TypCharGr");
        return u.loadMeasure(prop);
    }


    //---------------------- Typ For Select --------------------------------
    public Store loadTypForSelect(String lang) throws Exception {
        RelTypMemberMdbUtils ut = new RelTypMemberMdbUtils(getMdb());
        return ut.loadTypForSelect(lang);
    }
}
