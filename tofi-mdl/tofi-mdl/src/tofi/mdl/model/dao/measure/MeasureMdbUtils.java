package tofi.mdl.model.dao.measure;

import jandcode.commons.UtCnv;
import jandcode.core.auth.AuthService;
import jandcode.core.auth.AuthUser;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import tofi.mdl.consts.FD_PropType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.Map;

public class MeasureMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public MeasureMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }

    public Store load(Map<String, Object> params) throws Exception {
        long measureBase = UtCnv.toLong(params.get("measureBase"));
        long meter = UtCnv.toLong(params.get("meter"));

        String whe = "0=0";
        if (measureBase > 0) {
            whe = "id=" + measureBase + " or parent=" + measureBase;
        } else {
            if (meter > 0) {
                measureBase = mdb.loadQuery("select measure from Meter where id=:id ",
                        Map.of("id", meter)).get(0).getLong("measure");
                whe = "id=" + measureBase + " or parent=" + measureBase;
            }
        }
        //
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        //todo AuthUser
        long al = 10; //au.getAttrs().getLong("accesslevel");
        whe += " and accessLevel <= " + al;
        //
        Store st = mdb.createStore("Measure");
        String sql = "select * from Measure where " + whe;
        mdb.loadQuery(st, sql);
        mdb.resolveDicts(st);
        //
        return st;
    }

    public Store loadBase(Map<String, Object> params) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        //todo AuthUser
        long al = 10; //au.getAttrs().getLong("accesslevel");

        Store st = mdb.createStore("Measure");
        String sql = "select id, name from Measure where parent is null and accessLevel <= " + al;
        mdb.loadQuery(st, sql);
        //
        return st;
    }

    protected Store loadRec(long id) throws Exception {
        Store st = mdb.createStore("Measure");
        return mdb.loadQuery(st, """
                    select * from Measure where id=:id
                """, Map.of("id", id));
    }

    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntity(rec);

        //add to PropVal
        Store rTmp = mdb.loadQuery("select id, allItem from Prop where measure=:m and proptype=:pt",
                Map.of("m", id, "pt", FD_PropType_consts.measure));
        if (rTmp.size() > 0) {
            if (rTmp.get(0).getBoolean("allItem")) {
                long prop = rTmp.get(0).getLong("id");
                mdb.insertRec("PropVal", Map.of("prop", prop, "measure", id), true);
            }
        }
        return loadRec(id);
    }

    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        updateEntity(rec);
        //
        return loadRec(UtCnv.toLong(rec.get("id")));
    }

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }


}
