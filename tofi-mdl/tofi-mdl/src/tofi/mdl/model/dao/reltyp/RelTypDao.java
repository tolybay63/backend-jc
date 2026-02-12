package tofi.mdl.model.dao.reltyp;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class RelTypDao extends BaseModelDao {

    //------------------------------------- RelTyp -----------------------------------------

    public Map<String, Object> loadRelTypPaginate(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        return u.loadRelTypPaginate(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        u.delete(UtCnv.toMap(params.get("rec")));
    }

    public Store update(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        return u.update(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        return u.insert(params);
    }

    public Store loadRec(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        return u.loadRec(params);
    }

    //---------------------- RelTypVer --------------------------------
    public Store loadVer(long reltyp, String lang) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        return u.loadVer(reltyp, lang);
    }

    public Store updateVer(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        return u.updateVer(params);
    }

    public Store insertVer(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        return u.insertVer(params);
    }

    public void deleteVer(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        u.deleteVer(UtCnv.toMap(params.get("rec")));
    }

    //---------------------- TypRole --------------------------------
    public Store loadRelTypRole(long id, long reltyp, String lang) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        return u.loadRelTypRole(id, reltyp, lang);

    }

    public Store selectRelTypRole(long reltyp, String lang) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        return u.selectRelTypRole(reltyp, lang);
    }

    public Store insertRelTypRole(Map<String, Object> params) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        return u.insertRelTypRole(params);
    }

    public Store updateRelTypRole(Map<String, Object> params) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        return u.updateRelTypRole(params);
    }

    public void deleteRelTypRole(Map<String, Object> params) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        u.deleteRelTypRole(UtCnv.toMap(params.get("rec")));
    }

    //---------------------- TypRoleLifeInterval --------------------------------
    public Store loadRelTypRoleLife(long id, long reltypRole, String lang) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        return u.loadRelTypRoleLife(id, reltypRole, lang);
    }

    public Store insertRelTypRoleLife(Map<String, Object> params) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        return u.insertRelTypRoleLife(params);
    }

    public Store updateRelTypRoleLife(Map<String, Object> params) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        return u.updateRelTypRoleLife(params);
    }

    public void deleteRelTypRoleLife(Map<String, Object> params) throws Exception {
        RelTypRoleMdbUtils u = new RelTypRoleMdbUtils(getMdb());
        u.deleteRelTypRoleLife(UtCnv.toMap(params.get("rec")));
    }

    //---------------------- RelTypMember --------------------------------
    public Store loadRelTypMember(long reltyp, String lang) throws Exception {
        RelTypMemberMdbUtils u = new RelTypMemberMdbUtils(getMdb());
        return u.loadRelTypMember(reltyp, lang);
    }

    public Store insertRelTypMember(Map<String, Object> params) throws Exception {
        RelTypMemberMdbUtils u = new RelTypMemberMdbUtils(getMdb());
        return u.insertRelTypMember(params);
    }

    public Store updateRelTypMember(Map<String, Object> params) throws Exception {
        RelTypMemberMdbUtils u = new RelTypMemberMdbUtils(getMdb());
        return u.updateRelTypMember(params);
    }

    public void deleteRelTypMember(Map<String, Object> params) throws Exception {
        RelTypMemberMdbUtils u = new RelTypMemberMdbUtils(getMdb());
        u.deleteRelTypMember(UtCnv.toMap(params.get("rec")));
    }

    public void changeOrdMember(Map<String, Object> params) throws Exception {
        RelTypMemberMdbUtils u = new RelTypMemberMdbUtils(getMdb());
        u.changeOrdMember(params);
    }


    //RelTypCharGr
    public Store loadRelTypCharGr() throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.loadRelTypCharGr();
    }

    public StoreRecord loadRelTypCharGrInfo(long id) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.loadRelTypCharGrInfo(id);
    }

    public Store insertRelTypCharGr(Map<String, Object> rec) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.insertRelTypCharGr(rec);
    }

    public Store updateRelTypCharGr(Map<String, Object> rec) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.updateRelTypCharGr(rec);
    }

    public void deleteRelTypCharGr(long id) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        u.deleteRelTypCharGr(id);
    }

    public Store loadRelTypCharGrProp(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.loadRelTypCharGrProp(params);
    }

    public Store loadRelTypCharGrPropForUpd(long relTypCharGr) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.loadRelTypCharGrPropForUpd(relTypCharGr);
    }

    public String saveRelTypCharGrProps(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.saveRelTypCharGrProps(params);
    }

    public Store loadRelTypCharGrMultiProp(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.loadRelTypCharGrMultiProp(params);
    }

    public Store loadRelTypCharGrMultiPropForUpd(long relTypCharGr) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.loadRelTypCharGrMultiPropForUpd(relTypCharGr);
    }

    public String saveRelTypCharGrMultiProp(Map<String, Object> params) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.saveRelTypCharGrMultiProp(params);
    }

    public void updateRelTypCharGrMultiProp(Map<String, Object> rec) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        u.updateRelTypCharGrMultiProp(rec);
    }

    public void updateRelTypCharGrProp(Map<String, Object> rec) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        u.updateRelTypCharGrProp(rec);
    }

    public Store loadPropMeasure(long relTypCharGr) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.loadPropMeasure(relTypCharGr);
    }

    public Store loadMeasure(long prop) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTypCharGr");
        return u.loadMeasure(prop);
    }

    public Store loadRelTypForSelect(String lang) throws Exception {
        RelTypMemberMdbUtils ut = new RelTypMemberMdbUtils(getMdb());
        return ut.loadRelTypForSelect(lang);
    }

    public Store loadRelClsForSelect(long relTyp) throws Exception {
        RelTypMdbUtils u = new RelTypMdbUtils(getMdb(), "RelTyp");
        return u.loadRelClsForSelect(relTyp);
    }

}
