package tofi.mdl.model.dao.reltyp.relcls;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.List;
import java.util.Map;

public class RelClsDao extends BaseModelDao {


    public Store load(long id, long relTyp, String lang) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.load(id, relTyp, lang);
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.insert(rec);
    }

    public Store update(Map<String, Object> rec) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.update(rec);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        u.delete(rec);
    }

    public Store loadRec(long id, String lang) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.loadRec(id, lang);
    }

    public Store loadVer(long relcls, String lang) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.loadVer(relcls, lang);
    }

    public Store insertVer(Map<String, Object> rec) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.insertVer(rec);
    }

    public Store updateVer(Map<String, Object> rec) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.updateVer(rec);
    }

    public void deleteVer(Map<String, Object> rec) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        u.deleteVer(rec);
    }

    // RelClsMember

    public Store loadRelClsMember(long relcls, String lang) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.loadRelClsMember(relcls, lang);
    }

    public Store loadAllMembers(Map<String, Object> params) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.loadAllMembers(params);
    }

    public void createGroupRelCls(long relTyp, List<List<Map<String, Object>>> lists, long db, String lang) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        u.createGroupRelCls(relTyp, lists, db, lang);
    }

    public String deleteGroupRelCls(long relTyp, List<Map<String, Object>> list) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.deleteGroupRelCls(relTyp, list);
    }

    public Store updateRelClsMember(Map<String, Object> rec) throws Exception {
        RelClsMdbUtils u = new RelClsMdbUtils(getMdb(), "RelCls");
        return u.updateRelClsMember(rec);
    }
}
