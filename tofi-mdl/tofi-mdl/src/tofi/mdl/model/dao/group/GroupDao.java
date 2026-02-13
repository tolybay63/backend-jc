package tofi.mdl.model.dao.group;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class GroupDao extends BaseModelDao {

    public Store loadGroup(Map<String, Object> params) throws Exception {
        String tableName = UtCnv.toString(params.get("tableName"));
        GroupMdbUtils ut = new GroupMdbUtils(getMdb(), tableName);
        return ut.loadGroup(params);
    }

    public Store loadGroupForSelect(Map<String, Object> params) throws Exception {
        String tableName = UtCnv.toString(params.get("tableName"));
        GroupMdbUtils ut = new GroupMdbUtils(getMdb(), tableName);
        return ut.loadGroupForSelect(params);

    }

    public Store loadRec(Map<String, Object> params) throws Exception {
        String tableName = UtCnv.toString(params.get("tableName"));
        GroupMdbUtils u = new GroupMdbUtils(getMdb(), tableName);
        return u.loadRec(params);
    }

    public StoreRecord newRec(Map<String, Object> params) throws Exception {
        String tableName = UtCnv.toString(params.get("tableName"));
        GroupMdbUtils u = new GroupMdbUtils(getMdb(), tableName);
        return u.newRec();
    }

    public Store update(Map<String, Object> params) throws Exception {
        String tableName = UtCnv.toString(params.get("tableName"));
        GroupMdbUtils u = new GroupMdbUtils(getMdb(), tableName);
        return u.update(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        String tableName = UtCnv.toString(params.get("tableName"));
        GroupMdbUtils u = new GroupMdbUtils(getMdb(), tableName);
        return u.insert(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        String tableName = UtCnv.toString(params.get("tableName"));
        GroupMdbUtils u = new GroupMdbUtils(getMdb(), tableName);
        u.delete(params);
    }


}
