package tofi.mdl.model.dao.factor;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.commons.variant.VariantMap;
import jandcode.core.auth.AuthService;
import jandcode.core.auth.AuthUser;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.std.CfgService;
import jandcode.core.store.Store;
import tofi.api.dta.*;
import tofi.apinator.ApinatorApi;
import tofi.apinator.ApinatorService;
import tofi.mdl.consts.FD_PropType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class FactorMdbUtils extends EntityMdbUtils {

    ApinatorApi apiUserData() {
        return  mdb.getApp().bean(ApinatorService.class).getApi("userdata");
    }
    ApinatorApi apiNSIData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("nsidata");
    }
    ApinatorApi apiMonitoringData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("monitoringdata");
    }
    ApinatorApi apiObjectData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("objectdata");
    }
    ApinatorApi apiOrgStructureData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("orgstructuredata");
    }
    ApinatorApi apiPersonnalData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("personnaldata");
    }
    ApinatorApi apiPlanData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("plandata");
    }


    Mdb mdb;
    String tableName;

    public FactorMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }

    /**
     * Загрузка FactorVal без пагинацией
     *
     * @param params Map
     * @return Store
     */
    public Store loadFactorVal(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        long al = au.getAttrs().getLong("accesslevel");
        if (al==0)
            throw new XError("notLogined");
        String whe = "accessLevel<="+al+" and parent="+UtCnv.toLong(params.get("factor"));
        if (UtCnv.toLong(params.get("id"))>0) {
            whe = "accessLevel<="+al+" and id="+UtCnv.toLong(params.get("factor"));
        }

        Store st = mdb.createStore("Factor.lang");
        mdb.loadQuery(st, "select * from factor where " + whe + " order by ord");

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "Factor", UtCnv.toString(params.get("lang")));
    }

    /**
     * Загрузка Factor
     *
     * @param params Map
     * @return Store
     */
    public Store loadFactor(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        long al = au.getAttrs().getLong("accesslevel");
        if (al==0)
            throw new XError("notLogined");
        String whe = "parent is null and accessLevel <= "+al;
        if (UtCnv.toLong(params.get("id"))>0) {
            whe = "id="+UtCnv.toLong(params.get("id")) + " and accessLevel <= "+al;
        }

        String sql = "select * from factor where " + whe + " order by ord";

        Store st = mdb.createStore("Factor.lang");
        mdb.loadQuery(st, sql);

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "Factor", UtCnv.toString(params.get("lang")));
    }


    void is_exist_entity_as_data(long propVal, String metaModel) {
        List<String> lstApp = new ArrayList<>();
        if (metaModel.equalsIgnoreCase("fish")) {
            boolean b = apiUserData().get(ApiUserData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("userdata");
            b = apiNSIData().get(ApiNSIData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("nsidata");
            b = apiMonitoringData().get(ApiMonitoringData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("monitoringdata");
        }
        //...else if (metaModel.equalsIgnoreCase("kpi"))
        if (metaModel.equalsIgnoreCase("dtj")) {
            boolean b = apiUserData().get(ApiUserData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("userdata");
            b = apiNSIData().get(ApiNSIData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("nsidata");
            b = apiObjectData().get(ApiObjectData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("objectdata");
            b = apiOrgStructureData().get(ApiOrgStructureData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("orgstructuredata");
            b = apiPersonnalData().get(ApiPersonnalData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("personnaldata");
            b = apiPlanData().get(ApiPlanData.class).is_exist_entity_as_dataOld(0, "factorVal", propVal);
            if (b) lstApp.add("plandata");
        }

        String msg = UtString.join(lstApp, ", ");
        if (!lstApp.isEmpty())
            throw new XError("UseInApp@"+msg);
    }


    /**
     * Delete Factor
     *
     * @param rec record Factor
     */
    public void delete(Map<String, Object> rec) throws Exception {
        VariantMap map = new VariantMap(rec);
        if (map.getLong("parent") > 0) {
            //---< check data in other DB
            CfgService cfgSvc = mdb.getApp().bean(CfgService.class);
            String modelMeta = cfgSvc.getConf().getString("dbsource/default/id");
            if (modelMeta.isEmpty())
                throw new XError("Не найден id мета модели");

            long fv = UtCnv.toLong(rec.get("id"));
            Store stTmp = mdb.loadQuery("select id from PropVal where factorVal=:fv", Map.of("fv", fv));
            if (stTmp.size() > 0) {
                long propVal = stTmp.get(0).getLong("propVal");
                if (propVal > 0)
                    is_exist_entity_as_data(propVal, modelMeta);
            }
            //-->
        }

        deleteEntity(rec);
    }

    /**
     * Update Factor & FactorVal
     *
     * @param params Map
     * @return Store
     */
    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));

        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        updateEntity(rec);
        // Загрузка записи
        return loadFactor(rec);
    }

    /**
     * Insert Factor & FactorVal
     *
     * @param params Map
     * @return Store
     */
    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntity(rec);
        //add to PropVal
        long fac = UtCnv.toLong(rec.get("parent"));
        if (fac > 0) {
            Store rTmp = mdb.loadQuery("select id, allItem from Prop where factor=:f and proptype=:pt",
                    Map.of("f", fac, "pt", FD_PropType_consts.factor));
            if (rTmp.size() > 0) {
                if (rTmp.get(0).getBoolean("allItem")) {
                    long prop = rTmp.get(0).getLong("id");
                    mdb.insertRec("PropVal", Map.of("prop", prop, "factorVal", id), true);
                }
            }
        }
        //
        rec.put("id", id);
        return loadFactor(rec);
    }

    public Store loadRec(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        Store st = mdb.createStore("Factor.lang");
        mdb.loadQuery(st, "select * from factor where id=:id", Map.of("id", id));

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "Factor", UtCnv.toString(params.get("lang")));
    }

    public void changeOrdFV(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        long al = au.getAttrs().getLong("accesslevel");

        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        boolean up = UtCnv.toBoolean(params.get("up"));
        long factor = UtCnv.toLong(rec.get("parent"));
        long id1 = UtCnv.toLong(rec.get("id"));
        long ord1 = UtCnv.toLong(rec.get("ord"));
        long id2;
        long ord2;

        Store st = mdb.loadQuery("""
                    select * from Factor where parent=:factor and accessLevel < :al order by ord
                """, Map.of("factor", factor, "al", al));
        int k = 0;  //искомая позиция
        for (int i = 0; i < st.size(); i++) {
            if (st.get(i).getLong("id") == id1) {
                k = i;
                break;
            }
        }
        if (up) {
            id2 = st.get(k - 1).getLong("id");
            ord2 = st.get(k - 1).getLong("ord");
        } else {
            id2 = st.get(k + 1).getLong("id");
            ord2 = st.get(k + 1).getLong("ord");
        }
        //
        mdb.execQuery("""
                    update Factor set ord=:ord2 where id=:id1;
                    update Factor set ord=:ord1 where id=:id2;
                """, Map.of("id1", id1, "id2", id2, "ord1", ord1, "ord2", ord2));
    }

    public void changeOrdF(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        long al = au.getAttrs().getLong("accesslevel");

        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        boolean up = UtCnv.toBoolean(params.get("up"));
        long id1 = UtCnv.toLong(rec.get("id"));
        long ord1 = UtCnv.toLong(rec.get("ord"));
        long id2 = 0;
        long ord2 = 0;

        Store st = mdb.loadQuery("""
                    select * from Factor where parent is null and accessLevel<=:al order by ord
                """, Map.of("al", al));
        int k = 0;  //искомая позиция
        for (int i = 0; i < st.size(); i++) {
            if (st.get(i).getLong("id") == id1) {
                k = i;
                break;
            }
        }
        if (up) {
            id2 = st.get(k - 1).getLong("id");
            ord2 = st.get(k - 1).getLong("ord");
        } else {
            id2 = st.get(k + 1).getLong("id");
            ord2 = st.get(k + 1).getLong("ord");
        }
        //
        mdb.execQuery("""
                    update Factor set ord=:ord2 where id=:id1;
                    update Factor set ord=:ord1 where id=:id2;
                """, Map.of("id1", id1, "id2", id2, "ord1", ord1, "ord2", ord2));
    }

    public Store loadFactorTree(Map<String, Object> params) throws Exception {
        //
        long node = UtCnv.toLong(params.get("node"));
        String sql = "select * from factor where parent is null";
        if (node > 0)
            sql = "select * from factor where parent=" + node;

        Store st = mdb.createStore("Factor");
        mdb.loadQuery(st, sql);
        //mdb.resolveDicts(st);
        return st;
    }

    public Store loadForSelect() throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        //todo AuthUser
        long al = 10; //au.getAttrs().getLong("accesslevel");

        Store st = mdb.createStore("Factor.select");
        return mdb.loadQuery(st, """
                    select id, name from Factor where parent is null and accessLevel<=:al order by ord
                """, Map.of("al", al));
    }

    public Store getFactor(long fv) throws Exception {
        return mdb.loadQuery("""
                    with f as (
                        select parent as id from Factor where id=:id
                    )
                    select id, name from Factor where id in (select id from f)
                """, Map.of("id", fv));

    }

    public Store loadFvForSelect(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        //todo AuthUser
        long al = 10; //au.getAttrs().getLong("accesslevel");

        return mdb.loadQuery("""
                    select id, name, fullName, parent,
                        case when parent is null then -id else id end as factorval,
                        case when parent is null then null else 'factorval' end as ent
                    from Factor where accessLevel<=:al order by ord
                """, Map.of("al", al));
    }

}
