package tofi.mdl.model.dict


import jandcode.commons.error.XError
import jandcode.core.auth.AuthService
import jandcode.core.auth.AuthUser
import jandcode.core.dbm.dao.BaseModelDao
import jandcode.core.store.Store

class DictDao extends BaseModelDao {
    Store load(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("FD_DictsLang")
        String dict = params.get("dict").toString()
        if (!dict.equalsIgnoreCase("FD_AccessLevel")) {
            mdb.loadQuery(st, """
                select d.id, l.text
                from ${dict} d
                inner join FD_DictsLang l on d.id=l.idDict and l.nameDict=lower(:dict) and l.lang=:lang
                where d.vis=1
                order by d.ord
            """, params)

        } else {
            AuthService authSvc = mdb.getApp().bean(AuthService.class);
            AuthUser au = authSvc.getCurrentUser();
            long al = au.getAttrs().getLong("accesslevel");
            if (al==0)
                throw new XError("notLogined")
            params.put("al", al)
            mdb.loadQuery(st, """
                select d.id, l.text
                from FD_AccessLevel d
                inner join FD_DictsLang l on d.id=l.idDict and l.nameDict=lower(:dict) and l.lang=:lang
                where d.vis=1 and d.id<=:al 
                order by d.ord
            """, params)

        }
        //
        //mdb.outTable(st)
        return st;
    }

}
