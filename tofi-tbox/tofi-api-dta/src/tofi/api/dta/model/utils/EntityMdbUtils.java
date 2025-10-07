package tofi.api.dta.model.utils;

import jandcode.commons.*;
import jandcode.commons.datetime.XDateTime;
import jandcode.commons.datetime.XDateTimeFormatter;
import jandcode.commons.error.*;
import jandcode.core.dbm.domain.*;
import jandcode.core.dbm.mdb.*;
import jandcode.core.store.*;
import jandcode.core.dbm.mdb.BaseMdbUtils;

import java.util.*;

public class EntityMdbUtils extends BaseMdbUtils {
    Mdb mdb;
    String tableName;

    public EntityMdbUtils(Mdb mdb, String tableName) throws Exception {
        this.mdb = mdb;
        this.tableName = tableName;
    }

    public long insertEntity(Map<String, Object> rec) throws Exception {
        if (!UtCnv.toString(rec.get("tableName")).isEmpty())
            tableName = UtCnv.toString(rec.get("tableName"));
        //
        Store st = mdb.createStore(tableName);
        if (UtCnv.toString(rec.get("name")).isEmpty()) {
            throw new XError("Не указан [name]");
        }
        rec.putIfAbsent("accessLevel", 1L);
        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        StoreRecord r = st.add(rec);
        String cod = r.getString("cod");
        //
        long id1 = 0;
        if (tableName.equalsIgnoreCase("Obj")) {
            id1 = r.getLong("cls");
            if (id1==0L)
                throw new XError("NotFoundCls");
        }
        if (tableName.equalsIgnoreCase("RelObj")) {
            id1 = r.getLong("relcls");
            if (id1==0L)
                throw new XError("NotFoundRelCls");
        }
        //
        checkCod(cod);
        long id = mdb.getNextId(tableName);
        r.set("id", id);
        //
        DomainService domainSvc = mdb.getModel().bean(DomainService.class);
        Domain dm = domainSvc.getDomain(tableName);
        if (dm.findField("ord") != null) {
            r.set("ord", id);
        }
        //
        long ent = EntityConst.getNumConst(tableName);
        if (cod.isEmpty()) {
            cod = EntityConst.generateCod(ent, id1, id);
            r.set("cod", cod);
        }
        r.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME));
        //
        mdb.insertRec(tableName, r, false);
        // добавляем код
        mdb.insertRec("SysCod", Map.of("cod", cod, "entityType", ent, "entityId", id));
        //
        if (EntityConst.getEntityInfo(ent).getHasVer()) {
            st = mdb.createStore(tableName + "Ver");
            StoreRecord rV = st.add(rec);
            if (!r.isValueNull("cmt"))
                rV.set("cmtVer", r.get("cmt"));

            long idVer = mdb.getNextId(tableName + "Ver");
            rV.set("id", idVer);
            rV.set("ownerVer", id);
            rV.set("lastVer", 1);
            if (tableName.equalsIgnoreCase("obj")) {
                long parent = UtCnv.toLong(rec.get("parent"));
                if (parent > 0)
                    rV.set("objParent", parent);
            }
            mdb.insertRec(tableName + "Ver", rV, false);
        }
        //
        return id;
    }

    public void updateEntity(Map<String, Object> rec) throws Exception {
        if (!UtCnv.toString(rec.get("tableName")).isEmpty())
            tableName = UtCnv.toString(rec.get("tableName"));
        rec.putIfAbsent("accessLevel", 1L);
        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        //
        long id = UtCnv.toLong(rec.get("id"));
        DomainService domainSvc = mdb.getModel().bean(DomainService.class);
        Domain dm = domainSvc.getDomain(tableName);
        if (dm.findField("ord") != null) {
            rec.put("ord", id);
        }
        StoreRecord oldRec = mdb.loadQueryRecord("select cod from " + tableName + " where id=:id", Map.of("id", id));
        // Checking cod
        String cod = UtCnv.toString(rec.get("cod"));
        String oldCod = oldRec.getString("cod");
        //
        if (!UtString.empty(cod) && !cod.equalsIgnoreCase(oldCod)) {
            checkCod(cod);
        }
        //
        long id1 = 0;
        if (tableName.equalsIgnoreCase("Obj"))
            id1 = UtCnv.toLong(rec.get("cls"));
        if (tableName.equalsIgnoreCase("RelObj"))
            id1 = UtCnv.toLong(rec.get("relcls"));

        long ent = EntityConst.getNumConst(tableName);
        // генерим код, если не указан
        if (cod.isEmpty()) {
            cod = EntityConst.generateCod(ent, id1, id);
            rec.put("cod", cod);
        }
        // изменяем код
        if (!cod.equalsIgnoreCase(oldCod)) {
            mdb.execQuery("""
                        update SysCod set cod=:cod
                        where entityType=:entityType and entityId=:entityId
                    """, Map.of("cod", cod, "entityType", ent, "entityId", id));
        }
        //
        Store st = mdb.createStore(tableName);
        StoreRecord r = st.add(rec);
        if (dm.findField("timeStamp") != null)
            r.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME));
        mdb.updateRec(tableName, r);
        //
        if (EntityConst.getEntityInfo(ent).getHasVer()) {
            long verId = mdb.loadQuery("select v.id from " + tableName + " t," + tableName + "Ver v " +
                            "where t.id=v.ownerVer and v.lastVer=1 and t.id=:id",
                    Map.of("id", id)).get(0).getLong("id");

            st = mdb.createStore(tableName + "Ver");
            StoreRecord rV = st.add(rec);
            rV.set("id", verId);
            rV.set("ownerVer", id);
            rV.set("lastVer", 1);
            rV.set("cmtVer", rec.get("cmt"));
            if (tableName.equalsIgnoreCase("obj")) {
                long parent = UtCnv.toLong(rec.get("parent"));
                if (parent > 0)
                    rV.set("objParent", parent);
            }
            mdb.updateRec(tableName + "Ver", rV);
        }
    }

    protected void checkCod(String cod) throws Exception {
        if (cod.startsWith(EntityConst.genCodPref)) {
            throw new XError(UtLang.t("Код {0} не может начинаться с символа «_»"), cod);
        }
        if (!EntityConst.isCodValid(cod)) {
            throw new XError(UtLang.t("Допустимыми символами для кода являются: " +
                    "цифры, буквы на английском, «_», « - », « / », « . »"), cod);
        }
        checkCodUnique(cod);
    }

    /**
     * Проверка уникальности кода сущности
     */
    protected void checkCodUnique(String cod) throws Exception {
        Store ds = mdb.loadQuery("""                        
                    select entitytype, entityid from syscod
                    where UPPER(cod)=UPPER(:cod)
                """, Map.of("cod", cod));

        if (ds.size() > 0) {
            long entityType = ds.get(0).getLong("entitytype");
            long entityId = ds.get(0).getLong("entityid");
            EntityConst.EntityInfo entityInfo = EntityConst.getEntityInfo(entityType);
            String tableName = entityInfo.getTableName();
            String sql = "select name from " + tableName + " where id=:id";
            Store st = mdb.loadQuery(sql, Map.of("id", entityId));
            String inst = st.get(0).getString("name");
            throw new XError(UtLang.t("Введенный код является кодом экземпляра {1} сущности {0}", UtLang.t(entityInfo.getText()), inst), cod);
        }
    }

    public void deleteEntity(long id) throws Exception {

        String sign = tableName;
        long ent = EntityConst.getNumConst(sign);
        if (tableName.equalsIgnoreCase("RelObj")) {
            mdb.execQuery("""
                    delete from RelObjMember
                    where relobj=:id
                """, Map.of("id", id));
        }
        mdb.execQuery("""
                    delete from SysCod
                    where entityType=:entityType and entityId=:entityId
                """, Map.of("entityType", ent, "entityId", id));

        if (EntityConst.getEntityInfo(ent).getHasVer()) {
            mdb.execQuery("delete from " + tableName + "Ver where ownerVer=:id", Map.of("id", id));
        }
        //
        mdb.deleteRec(tableName, id);
    }

}
