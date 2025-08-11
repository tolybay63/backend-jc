package tofi.cube.dao.dimsOfCubeS

import jandcode.commons.datetime.XDate
import jandcode.commons.error.XError
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.ApiMetaCube
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class CubeSDimPeriod extends BaseMdbUtils {

    ApinatorApi apiMetaCube() {
        return app.bean(ApinatorService).getApi("meta")
    }

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    Mdb mdb
    private long cubeS
    private long dimPeriod

    CubeSDimPeriod(Mdb mdb, long cubeSId, long dimPeriodId) {
        this.mdb = mdb
        this.cubeS = cubeSId
        this.dimPeriod = dimPeriodId
    }


    void createTable() {
        String tbName = "cubeS_" + cubeS + "_dt_" + dimPeriod

        String sqlFields = """
            id varchar(120) not null,
            parent varchar(120),
            name varchar(800),     
            periodType bigint,
            dbeg date,
            dend date
        """
        String sqlTable = """
            create table ${tbName} (
                ${sqlFields}
            )
        """

        try {
            mdb.execQuery(sqlTable)
        } catch (Exception e) {
            e.printStackTrace()
        }

        //// Создание первичного ключа для таблицы измерения периодов
        String sql = "alter table ${tbName} add constraint pk_cubeS_${cubeS}_dt_${dimPeriod} primary key (id)"
        try {
            mdb.execQuery(sql)
        } catch (Exception e) {
            e.printStackTrace()
        }
    }

    void fillTable() {
        String sql = ""
        String sqlAdd = ""

        if (dimPeriod == 0) {
            String str = "'1800010133331231', '', 'Не зависит', 0, '1800-01-01', '3333-12-31' "
            sql = " select " + str
        } else {
            String curDte = XDate.today()
            Store stDimPeriod = apiMetaCube().get(ApiMetaCube).loadDimPeriod(dimPeriod, curDte)

            List<String> lst = new ArrayList<>()
            for (StoreRecord r in stDimPeriod) {
                String str = """
                select '${r.getString("id")}', '${r.get("parent")}', '${r.getString("name")}', ${r.getLong("periodType")}, 
                '${r.getDate("dbeg")}'::date, '${r.getDate("dend")}'::date
                """
                lst.add(str)
            }
            sql = lst.join(" union all ")

            /*
                Проверяем наличие свойств не зависящих от периода.
                Если такие свойства есть, то добавляем бесконечный период
            */

            Store stTmp = loadSqlMeta("""
                select c.prop, p.isdependvalueonperiod
                from CubeSPropCell c
                    left join Prop p on c.prop=p.id
                where c.cubeS=${cubeS} and p.isdependvalueonperiod=0
            """, "")

            if (stTmp.size()>0) {
                String str = """
                    '1800010133331231', '', 'Не зависит', 0, '1800-01-01', '3333-12-31'                
                """
                String sql0 = " select " + str
                sqlAdd = " union all " + sql0
            }
        }

        String tbName = "cubeS_" + cubeS + "_dt_" + dimPeriod

        sql = "insert into " + tbName + sql + sqlAdd

        mdb.execQuery(sql)

    }

    //
    private Store loadSqlMeta(String sql, String domain) {
        return apiMeta().get(ApiMeta).loadSql(sql, domain)
    }

}
