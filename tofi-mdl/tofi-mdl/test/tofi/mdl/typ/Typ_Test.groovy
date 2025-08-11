package tofi.mdl.typ

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test
import tofi.mdl.model.dao.typ.ClsTreeMdbUtils

class Typ_Test extends Apx_Test {

    @Test
    void test_1() throws Exception {

        ClsTreeMdbUtils ut = new ClsTreeMdbUtils(getMdb())

        //Store st = ut.loadClsTree([typ: 1001])
        //getMdb().outTable(st)

        Store st = ut.loadClsTree([typ: 1002])
        getMdb().outTable(st)

        st = ut.loadClsTree([typ: 1002, typNodeVisible: false])
        getMdb().outTable(st)

    }

    @Test
    void test_2() throws Exception {
        long reltyp = 1000

        Store stRTM = mdb.loadQuery("""
            select id, reltyp, card, membertype,
                case when membertype=1 then typ else reltypmemb end as ent, ord 
            from RelTypMember
            where reltyp=:rt
            order by ord
        """, [rt: reltyp])

        getMdb().outTable(stRTM)

        Store stRes = getMdb().createStore("RelClsMember.all")
        ClsTreeMdbUtils utCls = new ClsTreeMdbUtils(getMdb())

        int ind = 0
        for (StoreRecord r : stRTM) {
            ind++
            Store stCur = getMdb().createStore("RelClsMember.all")
            if (r.getLong("membertype") == 1) {
                Store stCls = utCls.loadClsTree([typ: r.getLong("ent")])
                stCur.add(stCls)
                for (StoreRecord rec : stCur) {
                    rec.set("memberType", 1)
                    rec.set("card", r.getInt("card"))
                    rec.set("checked", false)
                    rec.set("id", rec.getString("id") + "_" + ind)
                    if (!rec.get("parent"))
                        rec.set("ord", ind)
                    else
                        rec.set("parent", rec.getString("parent") + "_" + ind)
                }

            } else {
                Store stRelCls = mdb.loadQuery("""
                    select *, id as ent from RelCls
                    where relTyp=:rt
                    order by ord
                """, [rt: r.getLong("ent")])
                stCur.add(stRelCls)
                for (StoreRecord rec : stCur) {
                    rec.set("memberType", 2)
                    rec.set("card", r.getInt("card"))
                    rec.set("checked", false)
                    rec.set("id", rec.getString("id") + "_" + ind)
                }
            }
            stRes.add(stCur)
        }

        getMdb().outTable(stRes)

    }

    @Test
    public void testForMars() throws Exception {

        String sql = """
            select *, id as ent from RelCls
            where relTyp=:rt
            order by ord
        """
        Store stCur = getMdb().createStore("RelClsMember.all")
        getMdb().loadQuery(stCur, sql, [rt: 1000])
        getMdb().outTable(stCur)


    }

}
