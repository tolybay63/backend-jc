package tofi.mdl.model.dao.typ.test

import jandcode.commons.UtCnv
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.mdl.model.utils.EntityMdbUtils

class TestMdbUtils {
    Mdb mdb

    public TestMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb
        //
/*
        if (!mdb.getApp().getEnv().isTest())
            if (!UtCnv.toBoolean(mdb.createDao(AuthDao.class).isLogined().get("success")))
                throw new XError("notLogined");
*/
    }


    public void importObj(String path, String fn) throws Exception {

        File fle = new File(path + fn)

        fle.eachLine { it, n ->
            if (n > 1) {
                def arr = it.split("\t")
                parse(arr[0], arr[1], arr[2])
            }
        }

    }

    protected void parse(String sCls, String sParent, String name) throws Exception {
        long cls = UtCnv.toLong(sCls)
        long parent = UtCnv.toLong(sParent)

        Store st = mdb.createStore("Obj.full")
        StoreRecord r = st.add()
        r.set("accessLevel", 1)
        r.set("cls", cls)
        r.set("name", name)
        r.set("fullName", name)
        if (parent > 0)
            r.set("parent", parent)
        EntityMdbUtils eut = new EntityMdbUtils(mdb, "Obj")
        eut.insertEntityWithVer(r.getValues())

    }


    public void testFile(Map<String, Object> params) throws Exception {

        int o = 0
    }

}
