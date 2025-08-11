package tofi.mdl.model.dict

import jandcode.commons.UtCnv
import jandcode.commons.error.XError
import jandcode.core.auth.AuthService
import jandcode.core.auth.AuthUser
import jandcode.core.dbm.ModelService
import jandcode.core.dbm.dao.BaseModelDao
import jandcode.core.dbm.dict.DictData
import jandcode.core.dbm.dict.DictService
import jandcode.core.dbm.domain.Domain
import jandcode.core.store.Store

class DictDao extends BaseModelDao {

    Store load(Map<String, Object> params) throws Exception {
        ModelService modelSvc = getApp().bean(ModelService.class);
        DictService dictSvc =  modelSvc.getModel("default").bean(DictService.class);
        DictData dd = dictSvc.loadDictData(UtCnv.toString(params.get("dict")));
        //
        Domain dom = mdb.createDomain(dd.data)
        Store st = mdb.createStore(dom)
        if (!dd.name.equalsIgnoreCase("FD_AccessLevel")) {
            dd.data.forEach(r -> {
                if (r.getBoolean("vis"))
                    st.add(r)
            })
        } else {
            AuthService authSvc = mdb.getApp().bean(AuthService.class);
            AuthUser au = authSvc.getCurrentUser();
            long al = au.getAttrs().getLong("accesslevel");
            if (al==0)
                throw new XError("notLogined")
            st.clear()
            dd.data.forEach(r -> {
                if (r.getBoolean("vis") && r.getLong("id") <= al)
                    st.add(r)
            })
        }
        //
        return st;
    }


}
