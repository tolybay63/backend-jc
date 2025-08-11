package tofi.userdata.dao

import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.ModelService
import jandcode.core.dbm.dao.BaseModelDao
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store

class EntityDao extends BaseModelDao {

    @DaoMethod
    public void saveProfile(Map<String, Object> params) {
        EntityDaoUtils ut = new EntityDaoUtils(mdb, "Obj")
        ut.saveProfile(params)
    }

    @DaoMethod
    public void updateProfile(Map<String, Object> params) {
        EntityDaoUtils ut = new EntityDaoUtils(mdb, "Obj")
        ut.updateProfile(params)
    }

    @DaoMethod
    public Store insertGroup(Map<String, Object> params) {
        EntityDaoUtils ut = new EntityDaoUtils(mdb, "Obj")
        return ut.insertGroup(params)
    }

    @DaoMethod
    public Store updateGroup(Map<String, Object> params) {
        EntityDaoUtils ut = new EntityDaoUtils(mdb, "Obj")
        return ut.updateGroup(params)
    }

    @DaoMethod
    public void deleteGroup(long id) {
        EntityDaoUtils ut = new EntityDaoUtils(mdb, "Obj")
        ut.deleteGroup(id)
    }

    @DaoMethod
    void saveGroupUsers(long obj, /*long cls, */List<Map<String, Object>> lst) {
        EntityDaoUtils ut = new EntityDaoUtils(mdb, "Obj")
        ut.saveGroupUsers(obj, /*cls, */lst)
    }


}
