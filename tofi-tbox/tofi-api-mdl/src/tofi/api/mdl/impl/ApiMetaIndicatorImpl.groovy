package tofi.api.mdl.impl

import jandcode.commons.UtCnv
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import tofi.api.mdl.ApiMetaIndicator

class ApiMetaIndicatorImpl extends BaseMdbUtils implements ApiMetaIndicator {

    @Override
    Store loadMultiProp(long cls) {
        return mdb.loadQuery("""
            select * from (
                select -id as id, null as parent, name, ord
                from typchargr
                where typ in (select typ from Cls where id=:cls) 
                    and factorval in (select factorval from clsfactorval where cls=:cls)
                union all
                select mp.id, -tcp.typchargr as parent, mp.name, mp.ord
                from typchargrprop tcp 
                    left join MultiProp mp on tcp.multiprop=mp.id
                where tcp.typchargr in (
                        select id
                        from typchargr
                        where typ in (select typ from Cls where id=:cls) 
                        and factorval in (select factorval from clsfactorval where cls=:cls)
                    )
            ) t
            order by t.ord
        """, [cls: cls])
    }

    @Override
    Store loadDimMultiPropItem(long mp) {
        return mdb.loadQueryNative("""
            select dmpi.id, dmpi.parent, dmpi.name, dmpi.cod, null::float as val, null as dmpcell,
               m.name as nameMeasure, dmpim.measure, dmpim.maxval, dmpim.minval, m.kfrombase, dmpim.digit,
               t.mpd, dmpi.id as dmpi, t.dimNumber, '' || dmpi.id || '_' || t.mpd as idx
            from DimMultiPropItem dmpi
            left join DimMultiPropItemMeter dmpim on dmpim.dimMultiPropItem=dmpi.id 
            left join Measure m on dmpim.measure=m.id
            inner join (
                select dimmultiprop,multiProp, id as mpd, dimNumber
                from multipropdim
                where multiprop=${mp}
            ) t on t.dimmultiprop=dmpi.dimmultiprop 
            order by dmpi.ord
        """)
    }

    @Override
    Store loadMultiPropDim(long multiprop) {
        return mdb.loadQuery("""
            select *
            from multipropdim
            where multiprop=:mp
        """,[mp: multiprop])
    }

}
