package tofi.mdl.model.dao.dimobj;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_DimObjItemType_consts;
import tofi.mdl.consts.FD_LinkType_consts;
import tofi.mdl.consts.FD_MemberType_consts;
import tofi.mdl.model.utils.UtData;
import tofi.mdl.model.utils.tree.DataTreeNode;
import tofi.mdl.model.utils.tree.ITreeNodeVisitor;

import java.util.Map;

public class DimObjItemMdbUtils {
    Mdb mdb;

    public DimObjItemMdbUtils(Mdb mdb) {
        this.mdb = mdb;
        //

/*
        if (!mdb.getApp().getEnv().isTest())
            if (!UtCnv.toBoolean(mdb.createDao(AuthDao.class).isLogined().get("success")))
                throw new XError("notLogined");
*/
    }

    public Store loadDimObjItem(long dimobj) throws Exception {
        Store st = mdb.createStore("DimObjItem.full");
        return mdb.loadQuery(st, """
                    select d.*,
                        case
                        	when d.dimObjItemtype=1 then t.name
                        	when d.dimObjItemtype=2 then c.name
                        	when d.dimObjItemtype=3 then r.name
                        	when d.dimObjItemtype=4 then rc.name
                        	when d.dimObjItemtype=5 then tm.name
                        	when d.dimObjItemtype=7 then tm.name
                        	when d.dimObjItemtype=6 then cm.name
                        	when d.dimObjItemtype=8 then cm.name
                        end as name,
                        p.name as nameProp
                    from DimObjItem d
                        left join TypVer t on d.typ=t.ownerVer and t.lastVer=1
                        left join ClsVer c on d.cls=c.ownerVer and c.lastVer=1
                        left join RelTypVer r on d.relTyp=r.ownerVer and r.lastVer=1
                        left join RelClsVer rc on d.relCls=rc.ownerVer and rc.lastVer=1
                        left join RelTypMember tm on d.relTypMember=tm.id
                        left join RelClsMember cm on d.relClsMember=cm.id
                        left join Prop p on d.prop=p.id
                    where dimObj=:do
                """, Map.of("do", dimobj));
    }

    protected StoreRecord loadDimObjItemRec(long id) throws Exception {
        StoreRecord st = mdb.createStoreRecord("DimObjItem.full");
        return mdb.loadQueryRecord(st, """
                    select d.*,
                        case
                        	when d.dimObjItemtype=1 then t.name
                        	when d.dimObjItemtype=2 then c.name
                        	when d.dimObjItemtype=3 then r.name
                        	when d.dimObjItemtype=4 then rc.name
                        	when d.dimObjItemtype=5 then tm.name
                        	when d.dimObjItemtype=7 then tm.name
                        	when d.dimObjItemtype=6 then cm.name
                        	when d.dimObjItemtype=8 then cm.name
                        end as name
                    from DimObjItem d
                        left join TypVer t on d.typ=t.ownerVer and t.lastVer=1
                        left join ClsVer c on d.cls=c.ownerVer and c.lastVer=1
                        left join RelTypVer r on d.relTyp=r.ownerVer and r.lastVer=1
                        left join RelClsVer rc on d.relCls=rc.ownerVer and rc.lastVer=1
                        left join RelTypMember tm on d.relTypMember=tm.id
                        left join RelClsMember cm on d.relClsMember=cm.id
                    where d.id=:id
                """, Map.of("id", id));
    }

    protected StoreRecord loadRec(long id) throws Exception {
        Store st = mdb.createStore("DimObjItem");
        StoreRecord rec = st.add();
        return mdb.loadQueryRecord(rec, """
                    select * from DimObjItem where id=:id
                """, Map.of("id", id));
    }

    public Store loadTypForSelect(long relTyp, long parent, long doit, long linkType) throws Exception {
        //AuthService authSvc = mdb.getApp().bean(AuthService.class);
        //AuthUser au = authSvc.getCurrentUser();
        long al = 10; //au.getAttrs().getLong("accesslevel");
        if (parent == 0 || linkType == 1) {
            String sql = """
                            select t.id, v.name from Typ t, TypVer v
                            where t.id=v.ownerVer and v.lastVer=1 and t.accessLevel <= :al
                    """;
            if (doit == FD_DimObjItemType_consts.utyp) {
                sql = """
                            select id, name, typ from reltypmember
                            where reltyp=:rt and membertype=1
                            order by ord
                        """;
            }
            return mdb.loadQuery(sql, Map.of("al", al, "rt", relTyp));

        } else {
            StoreRecord recParent = loadRec(parent);
            String sql = "";
            long typORrel = 0;
            long parentDOIT = recParent.getLong("dimObjItemType");
            if (linkType == FD_LinkType_consts.p_c$urt) {
                //Typ, RelTyp
                if (parentDOIT == FD_DimObjItemType_consts.typ &&
                        doit == FD_DimObjItemType_consts.typ) {
                    typORrel = recParent.getLong("typ");
                    sql = """
                                select t.id, v.name from typ t, typver v
                                where t.id=v.ownerver and v.lastver=1
                                    and t.id <> :tr and t.id in (
                                        select typ from reltypmember where membertype=1
                                    )
                            """;
                } else if (parentDOIT == FD_DimObjItemType_consts.reltyp &&
                        doit == FD_DimObjItemType_consts.typ) {
                    sql = """
                                select t.id, v.name from reltyp t, reltypver v
                                where t.id=v.ownerver and v.lastver=1
                                    and t.id in (select reltypmemb from reltypmember where membertype=2)
                            """;
                }

                return mdb.loadQuery(sql, Map.of("al", al, "tr", typORrel));
            }
        }


        return null;
    }

/*
                if (linkType == 5) {
                    sql = """
                                with tt as (
                                    select distinct p.typ
                                    from typchargr c
                                        left join typchargrprop t on c.id=t.typchargr
                                        left join prop p on t.prop=p.id
                                    where c.typ=:tr and p.typ is not null
                                )
                                select t.id, v.name from Typ t, TypVer v
                                where t.id=v.ownerVer and v.lastVer=1 and t.accessLevel <= :al
                                    and t.id in (select typ from tt)
                            """;
                }
                //9 родительский компонент - значения одного или нескольких свойств дочернего компонента
                if (linkType == 9) {
                    sql = """
                                with tt as (
                                select distinct c.typ
                                from DataProp d, datapropval v, propVal pv, Obj o, Cls c
                                where d.id=v.dataprop and v.propval=pv.id and pv.obj is not null
                                    and pv.obj in (select id from Obj where cls in (select id from Cls where typ=:tr))
                                    and d.isobj=1 and d."owner"=o.id and o.cls=c.id
                                )
                                select t.id, v.name from Typ t, TypVer v
                                where t.id=v.ownerVer and v.lastVer=1 and t.accessLevel <= :al
                                    and t.id in (select typ from tt)
                            """;
                }
            } else if (parentDOIT == FD_DimObjItemType_consts.reltyp) {
                //6 дочерний компонент - значения одного или нескольких свойств участников родительского компонента
                typORrel = recParent.getLong("reltyp");
                if (linkType == 6) {

                    sql = """
                                with tt as (
                                    select distinct p.typ
                                    from typchargr t
                                        left join typchargrprop tp on t.id=tp.typchargr
                                        inner join Prop p on tp.prop=p.id and p.typ>0
                                    where t.typ in (select typ from reltypmember where reltyp=:tr)
                                )
                                select t.id, v.name from Typ t, TypVer v
                                where t.id=v.ownerVer and v.lastVer=1 and t.accessLevel <= :al
                                    and t.id in (select typ from tt)
                            """;
                }

                //11 участник(и) родительского компонента - значения одного или нескольких свойств дочернего компонента
                if (linkType == 11) {

                    sql = """
                                with tt as (
                                select distinct c.typ
                                from DataProp d, datapropval v, propVal pv, Obj o, Cls c
                                where d.id=v.dataprop and v.propval=pv.id and pv.obj is not null and d.isobj=1 and o.cls=c.id
                                    and pv.obj in (select id from Obj where cls in (select id from Cls where typ in
                                        (select typ from reltypmember where typ is not null and reltyp=:tr)))
                                )
                                select t.id, v.name from Typ t, TypVer v
                                where t.id=v.ownerVer and v.lastVer=1 and t.accessLevel <= :al
                                    and t.id in (select typ from tt)
                            """;
                }
*/


    public Store loadClsForSelect(long typ) throws Exception {
        // AuthService authSvc = mdb.getApp().bean(AuthService.class);
        // AuthUser au = authSvc.getCurrentUser();
        long al = 10; //au.getAttrs().getLong("accesslevel");

        return mdb.loadQuery("""
                    select t.id, v.name from Cls t, ClsVer v
                    where t.id=v.ownerVer and v.lastVer=1 and t.accessLevel <= :al and t.typ=:t
                """, Map.of("al", al, "t", typ));
    }

    public Store loadRelTypMember(long memberType, long relTyp,
                                  long parent, long linkType) throws Exception {
        String sql = "";
        long typORrel = 0;
        long reltypMemberParent = 0;
        long reltypParent = 0;

        if (linkType == FD_LinkType_consts.none) {
            sql = """
                        select id, name, case when typ is null then reltypMemb else typ end as typOrRel
                        from reltypmember
                        where membertype=:mt and reltyp=:rt
                        order by ord
                    """;
        } else {
            StoreRecord recParent = loadRec(parent);
            long parentDOIT = recParent.getLong("dimObjItemType");

            if (linkType == FD_LinkType_consts.p_c$urt) {  // linkType=2

                if (parentDOIT == FD_DimObjItemType_consts.typ) {
                    if (memberType == 1) {
                        typORrel = recParent.getLong("typ");
                        sql = """
                                    select id, name from reltypmember
                                    where membertype=1 and reltyp=:rt and typ <> :tORr
                                    order by ord
                                """;
                    }
                    if (memberType == 2) {
                        sql = """
                                    select id, name from reltypmember
                                    where membertype=2 and reltyp=:rt
                                    order by ord
                                """;
                    }

                }

                if (parentDOIT == FD_DimObjItemType_consts.reltyp) {
                    if (memberType == 1) {
                        sql = """
                                    select id, name from reltypmember
                                    where membertype=1 and reltyp=:rt
                                    order by ord
                                """;
                    }
                    if (memberType == 2) {
                        typORrel = recParent.getLong("reltyp");
                        sql = """
                                    select id, name from reltypmember
                                    where membertype=2 and reltyp=:rt and reltypmemb <> :tORr
                                    order by ord
                                """;
                    }

                }

                if (parentDOIT == FD_DimObjItemType_consts.utyp) {
                    reltypMemberParent = recParent.getLong("reltypMember");
                    sql = """
                                select id, name from Reltypmember where typ in (
                                    select typ
                                    from reltypmember
                                    where membertype=1 and id=:rtmParent
                                ) and id <> :rtmParent
                                order by ord
                            """;
                }
                if (parentDOIT == FD_DimObjItemType_consts.ureltyp) {
                    reltypMemberParent = recParent.getLong("reltypMember");
                    sql = """
                                select id, name from Reltypmember where reltypmemb in (
                                    select reltypmemb
                                    from reltypmember
                                    where membertype=2 and id=:rtmParent
                                ) and id <> :rtmParent
                                order by ord
                            """;
                }
            } else if (linkType == FD_LinkType_consts.c$up) {  // linkType=3
                if (parentDOIT == FD_DimObjItemType_consts.reltyp || parentDOIT == FD_DimObjItemType_consts.ureltyp) {
                    if (parentDOIT == FD_DimObjItemType_consts.reltyp)
                        reltypParent = recParent.getLong("relTyp");
                    else {
                        long rtm = recParent.getLong("reltypMember");
                        reltypParent = mdb.loadQueryRecord("select reltypmemb from RelTypMember where id=:id",
                                Map.of("id", rtm)).getLong("reltypmemb");
                    }
                    sql = """
                                select id, name from reltypmember where membertype=:mt and reltyp=:rtParent order by ord
                            """;
                }
            } else if (linkType == FD_LinkType_consts.c$val_up || linkType == FD_LinkType_consts.up$val_c) {  // linkType=6, 11

                if (parentDOIT == FD_DimObjItemType_consts.reltyp || parentDOIT == FD_DimObjItemType_consts.ureltyp) {
                    if (parentDOIT == FD_DimObjItemType_consts.reltyp)
                        reltypParent = recParent.getLong("relTyp");
                    else {
                        long rtm = recParent.getLong("reltypMember");
                        reltypParent = mdb.loadQueryRecord("select reltypmemb from RelTypMember where id=:id",
                                Map.of("id", rtm)).getLong("reltypmemb");
                    }

                    sql = """
                                select id, name, membertype,
                                case when membertype=1 then typ else reltypmemb end as typORrel
                                from reltypmember where relTyp=:rtParent
                            """;
                }

            } else if (linkType == FD_LinkType_consts.r$val_uc) {  // linkType=10
                sql = """
                            select id, name, membertype,
                                case when typ is null then reltypmemb else typ end as typORrel
                            from RelTypMember where reltyp=:rt
                        """;
            }
        }
        //


        return mdb.loadQuery(sql, Map.of("mt", memberType, "rt", relTyp,
                "rtmParent", reltypMemberParent, "tORr", typORrel, "rtParent", reltypParent));
    }


    public Store loadRelClsMember(long memberType, long relclsORreltyp, long parent, long linkType) throws Exception {
        String sql = "";

        if (linkType == FD_LinkType_consts.none) {
            sql = """
                        select id, name
                        from RelClsMember
                        where relcls = :rc and membertype=:mt
                        order by id
                    """;

            return mdb.loadQuery(sql, Map.of("mt", memberType, "rc", relclsORreltyp));

        } else {

            long clsORrel = 0;
            long relclsMemberParent = 0;
            StoreRecord recParent = loadRec(parent);
            long parentDOIT = recParent.getLong("dimObjItemType");


            if (linkType == FD_LinkType_consts.p_c$urt) {  // linkType=2
                if (parentDOIT == FD_DimObjItemType_consts.cls) {
                    if (memberType == 3) {
                        clsORrel = recParent.getLong("cls");
                        sql = """
                                    select id, name from relclsmember
                                    where membertype=3 and relcls=:rc and cls <> :cORr
                                    order by id
                                """;
                    }
                    if (memberType == 4) {
                        sql = """
                                    select id, name from relclsmember
                                    where membertype=4 and relcls=:rc
                                    order by id
                                """;
                    }

                }

                if (parentDOIT == FD_DimObjItemType_consts.relcls) {
                    if (memberType == 3) {
                        sql = """
                                    select id, name from relclsmember
                                    where membertype=3 and relcls=:rc
                                    order by id
                                """;
                    }
                    if (memberType == 4) {
                        clsORrel = recParent.getLong("relcls");
                        sql = """
                                    select id, name from relclsmember
                                    where membertype=4 and relcls=:rc and relclsmemb <> :cORr
                                    order by id
                                """;
                    }

                }

                if (parentDOIT == FD_DimObjItemType_consts.ucls) {
                    relclsMemberParent = recParent.getLong("relclsMember");
                    sql = """
                                select id, name from Relclsmember where cls in (
                                    select cls
                                    from relclsmember
                                    where membertype=3 and id=:rcmParent
                                ) and id <> :rcmParent
                                order by id
                            """;
                }
                if (parentDOIT == FD_DimObjItemType_consts.urelcls) {
                    relclsMemberParent = recParent.getLong("relclsMember");
                    sql = """
                                select id, name from Relclsmember where relclsmemb in (
                                    select relclsmemb
                                    from relclsmember
                                    where membertype=4 and id=:rcmParent
                                ) and id <> :rcmParent
                                order by id
                            """;
                }
            } else if (linkType == FD_LinkType_consts.c$up) {  // linkType=3 child uch of parent
                recParent = loadRec(parent);

                if (parentDOIT == FD_DimObjItemType_consts.relcls || parentDOIT == FD_DimObjItemType_consts.urelcls) {
                    if (parentDOIT == FD_DimObjItemType_consts.relcls)
                        relclsMemberParent = recParent.getLong("relcls");
                    else {
                        long rtm = recParent.getLong("relclsMember");
                        relclsMemberParent = mdb.loadQueryRecord("select relclsmemb from RelClsMember where id=:id",
                                Map.of("id", rtm)).getLong("relclsmemb");
                    }
                    sql = """
                                select id, name from RelClsMember where membertype=:mt and relcls=:rcmParent order by id
                            """;
                }

            } else if (linkType == FD_LinkType_consts.c$val_up || linkType == FD_LinkType_consts.up$val_c) {       //6, 11
                if (parentDOIT == FD_DimObjItemType_consts.relcls || parentDOIT == FD_DimObjItemType_consts.urelcls) {
                    if (parentDOIT == FD_DimObjItemType_consts.relcls)
                        relclsMemberParent = recParent.getLong("relCls");
                    else {
                        long rtm = recParent.getLong("relclsMember");
                        relclsMemberParent = mdb.loadQueryRecord("select relclsmemb from RelClsMember where id=:id",
                                Map.of("id", rtm)).getLong("relclsmemb");
                    }

                    sql = """
                                select id, name, membertype,
                                    case when membertype=3 then cls else relclsmemb end as clsORrel
                                from relclsmember where relCls=:rcmParent
                            """;
                }

            } else if (linkType == FD_LinkType_consts.r$val_uc) {       //10
                sql = """
                    select id, name, membertype, case when cls is null then relclsmemb else cls end as clsORrel
                    from RelClsMember where relcls=:rc
                """;

            }

            return mdb.loadQuery(sql, Map.of("mt", memberType, "rc", relclsORreltyp, "rcmParent", relclsMemberParent, "cORr", clsORrel));
        }

    }


    public Store loadRelTypForSelect(long doit, long parent, long linkType) throws Exception {
        //AuthService authSvc = mdb.getApp().bean(AuthService.class);
        //AuthUser au = authSvc.getCurrentUser();
        long al = 10; //au.getAttrs().getLong("accesslevel");
        String sql = "";
        if (parent == 0 || linkType == 1) {
            sql = """
                    select t.id, v.name from RelTyp t, RelTypVer v
                    where t.id=v.ownerVer and v.lastVer=1 and t.accessLevel <= :al
                """;
            //
            if (doit == FD_DimObjItemType_consts.utyp || doit == FD_DimObjItemType_consts.ureltyp) {
                long memberType = doit == FD_DimObjItemType_consts.utyp ? 1 : 2;
                sql = """
                            select t.id, v.name
                            from reltyp t, reltypver v
                            where t.accessLevel <= :al and t.id=v.ownerver and v.lastver=1
                                and t.id in (select reltyp from reltypmember where memberType=
                        """ + memberType + ");";
            }

            if (doit == FD_DimObjItemType_consts.ucls || doit == FD_DimObjItemType_consts.urelcls) {
                //long memberType = doit == FD_DimObjItemType_consts.ucls ? 3 : 4;
                sql = """
                            select t.id, v.name from reltyp t, RelTypVer v
                            where t.accessLevel <= :al and t.id=v.ownerver and v.lastver=1
                        """;
            }
            return mdb.loadQuery(sql, Map.of("al", al));
        }

        //2) родительский и дочерний компоненты - участники отношения между типами объектов;
        if (linkType == 2) {
            StoreRecord recParent = loadRec(parent);
            long typParent = 0;
            long reltypParent = 0;
            long reltypMemberParent = 0;
            long parentDOIT = recParent.getLong("dimObjItemType");

            if (doit == FD_DimObjItemType_consts.utyp || doit == FD_DimObjItemType_consts.ureltyp) {
                if (parentDOIT == FD_DimObjItemType_consts.typ) {
                    typParent = recParent.getLong("typ");
                    sql = """
                                select t.id, v.name
                                from reltyp t, reltypver v
                                where t.accessLevel <= :al and t.id=v.ownerver and v.lastver=1
                                    and t.id in (select reltyp from reltypmember where typ=:typParent)
                            """;
                } else if (parentDOIT == FD_DimObjItemType_consts.reltyp) {
                    reltypParent = recParent.getLong("reltyp");
                    sql = """
                                select t.id, v.name
                                from reltyp t, reltypver v
                                where t.accessLevel <= :al and t.id=v.ownerver and v.lastver=1
                                    and t.id in (select reltyp from reltypmember where reltypmemb=:reltypParent)
                            """;
                } else if (parentDOIT == FD_DimObjItemType_consts.utyp || parentDOIT == FD_DimObjItemType_consts.ureltyp) {
                    reltypMemberParent = recParent.getLong("reltypMember");
                    sql = """
                                select t.id, v.name
                                from reltyp t, reltypver v
                                where t.id=v.ownerver and v.lastver=1
                                    and t.id in (select reltyp from reltypmember where id=:rtmParent)
                            """;
                }
            }
            return mdb.loadQuery(sql, Map.of("al", al, "typParent", typParent, "reltypParent", reltypParent, "rtmParent", reltypMemberParent));
        }

        //todo Дочерний компонент - участник родительского компонента (id=3)
        //Absent!

        //Родительский компонент - участник дочернего компонента (id=4)
        if (linkType == 4) {
            StoreRecord recParent = loadRec(parent);
            long typParent = 0;
            long reltypParent = 0;
            long parentDOIT = recParent.getLong("dimObjItemType");

            if (parentDOIT == FD_DimObjItemType_consts.typ || parentDOIT == FD_DimObjItemType_consts.utyp) {
                if (parentDOIT == FD_DimObjItemType_consts.typ) {
                    typParent = recParent.getLong("typ");
                } else {
                    long rtm = recParent.getLong("reltypMember");
                    typParent = mdb.loadQueryRecord("select typ from RelTypMember where id=:id", Map.of("id", rtm))
                            .getLong("typ");
                }

                sql = """
                            select t.id, v.name
                            from RelTyp t, RelTypVer v
                            where t.id=v.ownerver and v.lastver =1 and t.id in (
                                select reltyp from reltypmember where membertype=1 and typ=:typ
                            )
                        """;
            }

            if (parentDOIT == FD_DimObjItemType_consts.reltyp || parentDOIT == FD_DimObjItemType_consts.ureltyp) {
                if (parentDOIT == FD_DimObjItemType_consts.reltyp) {
                    reltypParent = recParent.getLong("reltyp");
                } else {
                    long rtm = recParent.getLong("reltypMember");
                    reltypParent = mdb.loadQueryRecord("select reltypmemb from RelTypMember where id=:id", Map.of("id", rtm))
                            .getLong("reltypmemb");
                }

                sql = """
                            select t.id, v.name
                            from RelTyp t, RelTypVer v
                            where t.id=v.ownerver and v.lastver =1 and t.id in (
                                select reltyp from reltypmember where membertype=2 and reltypmemb=:reltyp
                            )
                        """;
            }


            return mdb.loadQuery(sql, Map.of("al", al, "typ", typParent, "reltyp", reltypParent));
        }

        if (linkType == 10) {
            return mdb.loadQuery("""
                        select t.id, v.name
                        from RelTyp t, RelTypVer v
                        where t.id=v.ownerver and v.lastver=1
                    """);
        }


        return null;
    }

    public Store loadRelClsForSelect(long relTyp, long doit, long parent, long linkType) throws Exception {
        String sql = "";
        if (linkType == FD_LinkType_consts.none) {
            sql = """
                        select c.id, v.name
                        from relcls c, relclsver v
                        where c.reltyp=:rt and c.id=v.ownerver and v.lastver=1
                        order by c.ord
                    """;

            return mdb.loadQuery(sql, Map.of("rt", relTyp));
        } else {

            StoreRecord recParent = loadRec(parent);
            long clsParent = 0;
            long relclsParent = 0;
            long relclsMemberParent = 0;
            long parentDOIT = recParent.getLong("dimObjItemType"); //cls, relcls, ucls,urelcls

            if (linkType == FD_LinkType_consts.p_c$urt) {   //2

                if (doit == FD_DimObjItemType_consts.ucls || doit == FD_DimObjItemType_consts.urelcls) {
                    if (parentDOIT == FD_DimObjItemType_consts.cls) {
                        clsParent = recParent.getLong("cls");
                        sql = """
                                    select t.id, v.name
                                    from relcls t, relclsver v
                                    where t.id=v.ownerver and v.lastver=1
                                        and t.id in (select relcls from relclsmember where cls=:clsParent)
                                """;
                    } else if (parentDOIT == FD_DimObjItemType_consts.relcls) {
                        relclsParent = recParent.getLong("relcls");
                        sql = """
                                    select t.id, v.name
                                    from relcls t, relclsver v
                                    where t.id=v.ownerver and v.lastver=1
                                        and t.id in (select relcls from relclsmember where relclsmemb=:relclsParent)
                                """;
                    } else if (parentDOIT == FD_DimObjItemType_consts.ucls || parentDOIT == FD_DimObjItemType_consts.urelcls) {
                        relclsMemberParent = recParent.getLong("relclsMember");
                        sql = """
                                    select t.id, v.name
                                    from relcls t, relclsver v
                                    where t.id=v.ownerver and v.lastver=1
                                        and t.id in (select relcls from relclsmember where id=:rcmParent)
                                """;
                    }
                }

            }
            //
            if (linkType == FD_LinkType_consts.p$uc) { //4
                if (parentDOIT == FD_DimObjItemType_consts.cls || parentDOIT == FD_DimObjItemType_consts.ucls) {
                    if (parentDOIT == FD_DimObjItemType_consts.cls) {
                        clsParent = recParent.getLong("cls");
                    } else {
                        long rtm = recParent.getLong("relclsMember");
                        clsParent = mdb.loadQueryRecord("select cls from RelClsMember where id=:id", Map.of("id", rtm))
                                .getLong("cls");
                    }

                    sql = """
                                select t.id, v.name
                                from RelCls t, RelClsVer v
                                where t.id=v.ownerver and v.lastver =1 and t.id in (
                                    select relcls from relClsmember where membertype=3 and cls=:clsParent
                                )
                            """;
                }

                if (parentDOIT == FD_DimObjItemType_consts.relcls || parentDOIT == FD_DimObjItemType_consts.urelcls) {
                    if (parentDOIT == FD_DimObjItemType_consts.relcls) {
                        relclsParent = recParent.getLong("relcls");
                    } else {
                        long rtm = recParent.getLong("relclsMember");
                        relclsParent = mdb.loadQueryRecord("select relclsmemb from RelClsMember where id=:id", Map.of("id", rtm))
                                .getLong("relclsmemb");
                    }

                    sql = """
                                select t.id, v.name
                                from RelCls t, RelClsVer v
                                where t.id=v.ownerver and v.lastver =1 and t.id in (
                                    select relcls from relclsmember where membertype=4 and relclsmemb=:relclsParent
                                )
                            """;
                }
            }

            if (linkType == FD_LinkType_consts.r$val_uc) { //10
                sql = """
                    select t.id, v.name
                    from RelCls t, RelClsVer v where t.id=v.ownerver and v.lastver=1 and reltyp=:relTyp
                """;
            }


            return mdb.loadQuery(sql, Map.of("clsParent", clsParent, "relclsParent", relclsParent,
                    "rcmParent", relclsMemberParent, "relTyp", relTyp));
        }


    }

    public StoreRecord insertDOI(Map<String, Object> params) throws Exception {
        StoreRecord r = mdb.createStoreRecord("DimObjItem", params);
        long id = mdb.insertRec("DimObjItem", r, true);
        return loadDimObjItemRec(id);
    }

    public StoreRecord updateDOI(Map<String, Object> params) throws Exception {
        StoreRecord r = mdb.createStoreRecord("DimObjItem", params);
        long id = r.getLong("id");
        mdb.updateRec("DimObjItem", r);
        return loadDimObjItemRec(id);
    }

    public void deleteDOI(long id) throws Exception {
        mdb.deleteRec("DimObjItem", id);
    }

    //-----------------------------------
    public Store loadDimObjItemProp(long dimObjItem) throws Exception {
        Store st = mdb.createStore("DimObjItemProp.full");
        return mdb.loadQuery(st, """
                    select d.*, p.name, f.name as nameStatus, c.name as nameProvider, p.propType, m.kfrombase, m.name as nameMeasure,
                        case when a.attribvaltype is null then 10*p.propType+p.propType else 10*p.propType+a.attribvaltype end as pt
                    from DimObjItemProp d
                        inner join Prop p on p.id=d.prop
                        left join Attrib a on p.attrib=a.id
                        left join measure m on p.measure=m.id
                        left join PropStatus ps on ps.id=d.propstatus
                        left join Factor f on ps.factorval=f.id
                        left join PropProvider pp on pp.id=d.propprovider
                        left join ClsVer c on pp.cls=c.ownerver and c.lastVer=1
                    where d.dimObjItem=:doi
                """, Map.of("doi", dimObjItem));
    }

    public Store loadPropStatus(long doi, long prop, String mode) throws Exception {
        String wh = " order by f.ord";
        if (mode.equals("ins"))
            wh = " and p.id not in (select coalesce(propStatus,0) from DimObjItemProp where dimObjItem=:doi) order by f.ord";

        return mdb.loadQuery("""
                    select p.id, f.name
                    from PropStatus p
                        left join Factor f on f.parent is not null and p.factorval=f.id
                    where prop=:p
                """ + wh, Map.of("p", prop, "doi", doi));
    }

    //Bad!!!
    public Store loadPropProvider(long prop) throws Exception {
        Store st = mdb.createStore("DimObjItem.status.provider");
        mdb.loadQuery(st, """
                    select distinct
                        p.id,
                        case
                            when p.obj is not null then ov."name"
                            when p.cls is not null then ov2."name"
                        end as name
                    from PropProvider p
                        left join Obj o on p.obj=o.id
                        left join ObjVer ov on o.id=ov.ownerVer and ov.lastVer=1
                        left join Cls c on p.cls=c.id
                        left join Obj o2 on p.cls=o2.cls
                        left join ObjVer ov2 on o2.id=ov2.ownerVer and ov2.lastVer=1
                    where p.prop=:p
                """, Map.of("p", prop));
        return st;
    }

    public Store loadOptProp(long doi) throws Exception {
        //AuthService authSvc = mdb.getApp().bean(AuthService.class);
        //AuthUser au = authSvc.getCurrentUser();
        long al = 10; //au.getAttrs().getLong("accesslevel");

        StoreRecord recParent = null;
        StoreRecord recDOI = loadRec(doi);
        if (recDOI.getLong("parent") > 0) {
            recParent = loadRec(recDOI.getLong("parent"));
        }
        long doitType = recDOI.getLong("dimObjItemType");
        long linkType = recDOI.getLong("linkType");
        String whe = "";

        String sql = "";

        if (linkType < 5) {
            long typ = 0;
            long cls = 0;
            if (doitType == FD_DimObjItemType_consts.typ || doitType == FD_DimObjItemType_consts.cls) {
                typ = recDOI.getLong("typ");
                cls = recDOI.getLong("cls");
                if (cls > 0) {
                    whe = " c.cls=:cls)) order by ord;";
                } else {
                    whe = " tcg.typ=:typ)) order by ord;";
                }

                sql = """
                            select p.id, p.name, p.statusfactor, p.providertyp, p.propType, a.attribvaltype
                            from Prop p
                                left join Attrib a on p.attrib=a.id
                                left join measure m on p.measure=m.id
                            where (a.attribvaltype is null or a.attribvaltype < 9) and p.propType < 8 and
                            p.id in (
                                select prop
                                from typchargrprop p
                                where p.prop is not null and p.typchargr in (
                                    select tcg.id from clsfactorval c, typchargr tcg
                                    where c.factorval=tcg.factorval and
                        """ + whe;

            } else if (doitType == FD_DimObjItemType_consts.reltyp || doitType == FD_DimObjItemType_consts.relcls) {
                typ = recDOI.getLong("reltyp");
                cls = recDOI.getLong("relcls");
                if (cls > 0) {
                    whe = " relcls=:cls )) order by ord;";
                } else {
                    whe = " relcls in (select id from relcls where reltyp=:typ))) order by ord;";
                }

                sql = """
                            select p.id, p.name, p.statusfactor, p.providertyp, p.propType, a.attribvaltype\s
                            from Prop p
                                left join Attrib a on p.attrib=a.id
                                left join measure m on p.measure=m.id
                            where
                            (a.attribvaltype is null or a.attribvaltype < 9) and
                            p.id in (
                                select prop
                                from reltypchargrprop p
                                where p.prop is not null and p.reltypchargr in (
                                    select id from reltypchargr where
                        """ + whe;
            } else if (doitType == FD_DimObjItemType_consts.utyp) {
                long rtm = recDOI.getLong("relTypMember");
                typ = mdb.loadQuery("select typ from reltypmember where id=:id",
                        Map.of("id", rtm)).get(0).getLong("typ");

                sql = """
                            select p.id, p.name, p.statusfactor, p.providertyp, p.propType, a.attribvaltype
                            from Prop p
                                left join Attrib a on p.attrib=a.id
                                left join measure m on p.measure=m.id
                            where (a.attribvaltype is null or a.attribvaltype < 9) and p.propType < 8 and
                            p.id in (
                                select prop
                                from typchargrprop p
                                where p.prop is not null and p.typchargr in (
                                    select tcg.id from clsfactorval c, typchargr tcg
                                    where c.factorval=tcg.factorval and tcg.typ=:typ))
                                order by ord;
                        """;
            } else if (doitType == FD_DimObjItemType_consts.ureltyp) {
                long rtm = recDOI.getLong("relTypMember");
                //reltyp
                typ = mdb.loadQuery("select reltypmemb from reltypmember where id=:id",
                        Map.of("id", rtm)).get(0).getLong("reltypmemb");
                sql = """
                            select p.id, p.name, p.statusfactor, p.providertyp, p.propType, a.attribvaltype\s
                            from Prop p
                                left join Attrib a on p.attrib=a.id
                                left join measure m on p.measure=m.id
                            where
                            (a.attribvaltype is null or a.attribvaltype < 9) and
                            p.id in (
                                select prop
                                from reltypchargrprop p
                                where p.prop is not null and p.reltypchargr in (
                                    select id from reltypchargr where relcls in
                                        (select id from relcls where reltyp=:typ))) order by ord;
                        """;
            } else if (doitType == FD_DimObjItemType_consts.ucls) {
                long rcm = recDOI.getLong("relClsMember");
                cls = mdb.loadQuery("select cls from relclsmember where id=:id",
                        Map.of("id", rcm)).get(0).getLong("cls");

                sql = """
                                select p.id, p.name, p.statusfactor, p.providertyp, p.propType, a.attribvaltype
                                from Prop p
                                    left join Attrib a on p.attrib=a.id
                                    left join measure m on p.measure=m.id
                                where (a.attribvaltype is null or a.attribvaltype < 9) and p.propType < 8 and
                                p.id in (
                                	select prop
                                	from typchargrprop p
                                	where p.prop is not null and p.typchargr in (
                                		select tcg.id from clsfactorval c, typchargr tcg
                                		where c.factorval=tcg.factorval and c.cls=:cls ))
                                order by ord;
                        """;
            } else if (doitType == FD_DimObjItemType_consts.urelcls) {
                long rcm = recDOI.getLong("relClsMember");
                cls = mdb.loadQuery("select relclsmemb from relclsmember where id=:id",
                        Map.of("id", rcm)).get(0).getLong("relclsmemb");
                sql = """
                            select p.id, p.name, p.statusfactor, p.providertyp, p.propType, a.attribvaltype\s
                            from Prop p
                                left join Attrib a on p.attrib=a.id
                                left join measure m on p.measure=m.id
                            where
                            (a.attribvaltype is null or a.attribvaltype < 9) and
                            p.id in (
                                select prop
                                from reltypchargrprop p
                                where p.prop is not null and p.reltypchargr in (
                                    select id from reltypchargr where relcls=:cls ))
                            order by ord;
                        """;
            }
            return mdb.loadQuery(sql, Map.of("typ", typ, "cls", cls));
        }


        long typORrel = 0;
        long parentRel = 0;
        if (recParent != null) {
            if (linkType == 5) {
                if (recParent.getLong("typ") > 0) { //parentDOIT: typ,cls,obj
                    typORrel = recParent.getLong("typ");
                    sql = """
                                with pp as (
                                    select distinct p.id
                                    from typchargr c
                                        left join typchargrprop t on c.id=t.typchargr
                                        left join prop p on t.prop=p.id
                                    where c.typ=:rt and (p.typ is not null or p.reltyp is not null)
                                )
                                select id, name from Prop
                                where id in (select id from pp) and accessLevel <= :al
                            """ + whe;
                } else {
                    typORrel = recDOI.getLong("reltyp");
                    sql = """
                                with pp as (
                                    select distinct p.id
                                    from reltypchargr c
                                        left join reltypchargrprop t on c.id=t.reltypchargr
                                        left join prop p on t.prop=p.id
                                    where c.reltyp=:rt and (p.typ is not null or p.reltyp is not null)
                                )
                                select id, name from Prop
                                where id in (select id from pp) and accessLevel <= :al
                            """ + whe;
                }
            } else if (linkType == 9) {
                if (recParent.getLong("typ") > 0) { //parentDOIT: typ,cls,obj
                    typORrel = recParent.getLong("typ");
                    sql = """
                                with pp as (
                                select distinct d.prop
                                from DataProp d, datapropval v, propVal pv, Obj o, Cls c
                                where d.id=v.dataprop and v.propval=pv.id and pv.obj is not null
                                    and pv.obj in (select id from Obj where cls in (select id from Cls where typ=:rt))
                                    and d.isobj=1 and d."owner"=o.id and o.cls=c.id
                                )
                                select id, name from Prop
                                where id in (select prop from pp) and accessLevel <= :al
                            """ + whe;
                } else {
                    typORrel = recDOI.getLong("reltyp");
                    sql = """
                                with pp as (
                                select distinct d.prop
                                from DataProp d, datapropval v, propVal pv, RelObj o
                                where d.id=v.dataprop and v.propval=pv.id and pv.relobj is not null
                                    and pv.relobj in (select id from RelObj where reltyp=:rt)
                                    and d.isobj=0 and d."owner"=o.id
                                )
                                select id, name from Prop
                                where id in (select prop from pp) and accessLevel <= :al
                            """ + whe;
                }
            } else if (linkType == 6) {
                typORrel = recDOI.getLong("typ");
                parentRel = recParent.getLong("reltyp");

                sql = """
                            with mm as (
                                select distinct a.id as rtm, p.prop, t.typ
                                from typchargr t
                                left join typchargrprop p on p.typchargr=t.id
                                inner join (select id, typ from reltypmember where reltyp=:parentRel) a on a.typ=t.typ
                                where 0=0
                            ),
                            pp as (
                                select distinct d.prop, mm.rtm
                                from DataProp d, datapropval v, propVal pv, Obj o, Cls c, mm
                                where d.id=v.dataprop and v.propval=pv.id and pv.obj is not null
                                    and pv.obj in (select id from Obj where cls in (select id from Cls where typ=:rt))
                                    and d.isobj=1 and d."owner"=o.id and o.cls=c.id and d.prop=mm.prop
                            )
                            select id, name, pp.rtm from Prop, pp
                            where id=pp.prop and  accessLevel <= :al
                        """ + whe;
            }
        }
        //
        return mdb.loadQuery(sql, Map.of("rt", typORrel, "al", al, "doi", doi, "parentRel", parentRel));
    }

    public Store loadDimObjItemPropRec(long id) throws Exception {
        Store st = mdb.createStore("DimObjItemProp.full");
        return mdb.loadQuery(st, """
                    select d.*, p.name, f.name as nameStatus, c.name as nameProvider, p.propType, m.kfrombase, m.name as nameMeasure,
                        case when a.attribvaltype is null then 10*p.propType+p.propType else 10*p.propType+a.attribvaltype end as pt
                    from DimObjItemProp d
                        inner join Prop p on p.id=d.prop
                        left join Attrib a on p.attrib=a.id
                        left join measure m on p.measure=m.id
                        left join PropStatus ps on ps.id=d.propstatus
                        left join Factor f on ps.factorval=f.id
                        left join PropProvider pp on pp.id=d.propprovider
                        left join ClsVer c on pp.cls=c.ownerver and c.lastVer=1
                    where d.id=:id
                """, Map.of("id", id));
    }


    public Store insertDOIprop(Map<String, Object> params) throws Exception {
        StoreRecord r = mdb.createStoreRecord("DimObjItemProp", params);
        long id = mdb.insertRec("DimObjItemProp", r, true);
        //
        return loadDimObjItemPropRec(id);
    }

    public Store updateDOIprop(Map<String, Object> params) throws Exception {
        StoreRecord r = mdb.createStoreRecord("DimObjItemProp", params);
        mdb.updateRec("DimObjItemProp", r);
        //
        return loadDimObjItemPropRec(r.getLong("id"));
    }

    public void deleteDOIprop(long id) throws Exception {
        mdb.deleteRec("DimObjItemProp", id);
    }

    public Store loadDimObjItemPropVal(long dimObjItemProp, long prop, long pt) throws Exception {
        Store st = mdb.createStore("DimObjItemPropVal");

        mdb.loadQuery(st, """
                    select * from DimObjItemPropVal where dimObjItemProp=:doip
                """, Map.of("doip", dimObjItemProp));

        if (st.size() > 0) {
            if (pt == 22 || pt == 23) {
                long ptt = pt == 22 ? 2 : 3;
                Store stMea = mdb.loadQuery("""
                            select m.kfrombase
                            from Prop p, Measure m
                            where p.id=:p and p.propType=:pt and p.measure=m.id
                        """, Map.of("p", prop, "pt", ptt));
                double kf = stMea.get(0).getDouble("kfrombase");
                if (kf==0) kf = 1;
                //
                st.get(0).set("numberVal", st.get(0).getDouble("numberVal") / kf);
            }
        }
        //
        return st;
    }

    public Store loadOptPropVal(long prop, long pt, long doip, String mode) throws Exception {
        String whe = "";
        if (mode.equals("ins")) {
            whe = " and p.id not in (select propVal from DimObjItemPropVal where dimObjItemProp=:doip)";
        }

        String sql = """
                    select p.id, f.name
                    from PropVal p, Factor f
                    where prop=:prop and p.factorval=f.id
                """ + whe + " order by f.ord";
        if (pt == 55) {
            sql = """
                        select id, obj, cls
                        from PropVal
                        where prop=:prop and obj is not null
                    """ + whe;
        } else if (pt == 66) {
            sql = """
                        select id, relobj, relcls
                        from PropVal
                        where prop=:prop and relobj is not null
                    """ + whe;
        } else if (pt == 77) {
            sql = """
                        select p.id, m.name
                        from PropVal p, measure m
                        where prop=:prop and p.measure=m.id
                    """ + whe;
        }

        return mdb.loadQuery(sql, Map.of("prop", prop, "doip", doip));
    }

    public Store loadOptForRefValues(String ent) throws Exception {
        String sql = "";
        if (ent.equals("fv")) {
            sql = """
                        select p.id, f.name
                        from PropVal p, factor f
                        where p.factorval=f.id
                    """;
        } else if (ent.equals("obj")) {
            sql = """
                        select p.id, o.name
                        from PropVal p, ObjVer o
                        where p.obj=o.ownerVer and o.lastVer=1
                    """;
        } else if (ent.equals("relobj")) {
            sql = """
                        select p.id, o.name
                        from PropVal p, RelObjVer o
                        where p.relobj=o.ownerVer and o.lastVer=1
                    """;
        } else if (ent.equals("measure")) {
            sql = """
                        select p.id, m.name
                        from PropVal p, measure m
                        where p.measure=m.id
                    """;
        }
        //
        return mdb.loadQuery(sql);
    }

    public void insertDOIpropValue(Map<String, Object> rec) throws Exception {
        StoreRecord r = mdb.createStoreRecord("DimObjItemPropVal", rec);
        mdb.insertRec("DimObjItemPropVal", r, true);
    }

    public void updateDOIpropValue(Map<String, Object> rec) throws Exception {
        StoreRecord r = mdb.createStoreRecord("DimObjItemPropVal", rec);
        mdb.updateRec("DimObjItemPropVal", r);
    }

    public void deleteDOIpropValue(long id) throws Exception {
        mdb.deleteRec("DimObjItemPropVal", id);
    }

    public Store loadProp_10_11(long doit, long rtmORrcm, long parent, long linkType) throws Exception {
        StoreRecord recParent = loadRec(parent);
        long parentDOIT = recParent.getLong("dimObjItemType");
        long parentTyp = 0;
        long parentCls = 0;
        long parentRelTyp = 0;
        long parentRelCls = 0;
        long typORrel = 0;
        long clsORrel = 0;
        String sql = "";
        if (parentDOIT==FD_DimObjItemType_consts.typ || parentDOIT==FD_DimObjItemType_consts.utyp) {
            if (parentDOIT == FD_DimObjItemType_consts.typ)
                parentTyp = recParent.getLong("typ");
            else {
                long rtm = recParent.getLong("reltypMember");
                parentTyp = mdb.loadQueryRecord("select typ from RelTypMember where id=:id",
                        Map.of("id", rtm)).getLong("typ");
            }
            if (doit==FD_DimObjItemType_consts.reltyp) {
                StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select typ, reltypmemb from RelTypMember where id=:id",
                        Map.of("id", rtmORrcm));

                if (rtmORrcmRecord.getLong("reltypmemb") == 0) {
                    typORrel = rtmORrcmRecord.getLong("typ");
                    sql = """
                                select distinct pp.id, pp.name
                                from typchargr t
                                    left join typchargrprop p on t.id=p.typchargr
                                    inner join Prop pp on pp.id=p.prop and pp.proptype=5 and pp.typ=:parentTyp
                                where t.typ=:typORrel
                            """;
                } else {
                    typORrel = rtmORrcmRecord.getLong("reltypmemb");
                    sql = """
                                select  distinct pp.id, pp.name
                                from reltypchargr t
                                    left join reltypchargrprop p on t.id=p.reltypchargr
                                    inner join Prop pp on pp.id=p.prop and pp.proptype=5 and pp.typ=:parentTyp
                                where t.relcls in (select id from RelCls where reltyp=:typORrel)
                            """;
                }
            } else if (doit==FD_DimObjItemType_consts.relcls) {
                StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select cls, relclsmemb from RelClsMember where id=:id",
                        Map.of("id", rtmORrcm));
                if (rtmORrcmRecord.getLong("relclsmemb") == 0) {
                    clsORrel = rtmORrcmRecord.getLong("cls");
                    sql = """
                            select distinct pp.id, pp.name
                            from typchargr t
                            	left join typchargrprop p on t.id=p.typchargr
                            	inner join Prop pp on pp.id=p.prop and pp.proptype=5 and pp.typ=:parentTyp
                            where t.typ in (select typ from Cls where id=:clsORrel)
                            	and t.factorval in (select factorval from clsfactorval where cls=:clsORrel)
                    """;
                } else {
                    clsORrel = rtmORrcmRecord.getLong("relclsmemb");
                    sql = """
                            select  distinct pp.id, pp.name
                            from reltypchargr t
                            	left join reltypchargrprop p on t.id=p.reltypchargr
                            	inner join Prop pp on pp.id=p.prop and pp.proptype=5 and pp.typ=:parentTyp
                            where t.relcls=:clsORrel
                    """;
                }
            }
        }
        if (parentDOIT==FD_DimObjItemType_consts.cls || parentDOIT==FD_DimObjItemType_consts.ucls) {
            if (parentDOIT == FD_DimObjItemType_consts.cls)
                parentCls = recParent.getLong("cls");
            else {
                long rtm = recParent.getLong("relclsMember");
                parentCls = mdb.loadQueryRecord("select relclsmemb from RelClsMember where id=:id",
                        Map.of("id", rtm)).getLong("relclsmemb");
            }
            if (doit==FD_DimObjItemType_consts.reltyp) {
                StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select typ, reltypmemb from RelTypMember where id=:id",
                        Map.of("id", rtmORrcm));

                if (rtmORrcmRecord.getLong("reltypmemb") == 0) {
                    typORrel = rtmORrcmRecord.getLong("typ");
                    sql = """
                            select distinct pp.id, pp.name
                            from typchargr t
                            	left join typchargrprop p on t.id=p.typchargr
                            	inner join Prop pp on pp.id=p.prop and pp.proptype=5
                            	left join PropVal pv on pp.id=pv.prop and pv.cls=:parentCls
                            where t.typ=:typORrel
                    """;
                } else {
                    typORrel = rtmORrcmRecord.getLong("reltypmemb");
                    sql = """
                            select  distinct pp.id, pp.name
                            from reltypchargr t
                            	left join reltypchargrprop p on t.id=p.reltypchargr
                            	inner join Prop pp on pp.id=p.prop and pp.proptype=5
                            	left join PropVal pv on pp.id=pv.prop and pv.cls=:parentCls
                            where t.relcls in (select id from RelCls where reltyp=:typORrel)
                    """;
                }
            } else if (doit==FD_DimObjItemType_consts.relcls) {
                StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select cls, relclsmemb from RelClsMember where id=:id",
                        Map.of("id", rtmORrcm));
                if (rtmORrcmRecord.getLong("relclsmemb") == 0) {
                    clsORrel = rtmORrcmRecord.getLong("cls");
                    sql = """
                            select distinct pp.id, pp.name
                            from typchargr t
                            	left join typchargrprop p on t.id=p.typchargr
                            	inner join Prop pp on pp.id=p.prop and pp.proptype=5 and pp.typ in (select typ from Cls where id=:parentCls)
                            	left join PropVal pv on pp.id=pv.prop and pv.cls=:parentCls
                            where t.typ in (select typ from Cls where id=:clsORrel)
                            	and t.factorval in (select factorval from clsfactorval where cls=:clsORrel)
                    """;
                } else {
                    clsORrel = rtmORrcmRecord.getLong("relclsmemb");
                    sql = """
                            select  distinct pp.id, pp.name
                            from reltypchargr t
                            	left join reltypchargrprop p on t.id=p.reltypchargr
                            	inner join Prop pp on pp.id=p.prop and pp.proptype=5 and pp.typ in (select typ from Cls where id=:parentCls)
                            where t.relcls = :clsORrel
                    """;
                }
            }
        }

        if (parentDOIT==FD_DimObjItemType_consts.reltyp || parentDOIT==FD_DimObjItemType_consts.ureltyp) {
            if (parentDOIT == FD_DimObjItemType_consts.reltyp)
                parentRelTyp = recParent.getLong("reltyp");
            else {
                long rtm = recParent.getLong("reltypMember");
                parentRelTyp = mdb.loadQueryRecord("select reltypmemb from RelTypMember where id=:id",
                        Map.of("id", rtm)).getLong("reltypmemb");
            }

            if (linkType == 10) {
                if (doit == FD_DimObjItemType_consts.reltyp) {
                    StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select typ, reltypmemb from RelTypMember where id=:id",
                            Map.of("id", rtmORrcm));


                    if (rtmORrcmRecord.getLong("reltypmemb") == 0) {
                        typORrel = rtmORrcmRecord.getLong("typ");
                        sql = """
                                        select distinct pp.id, pp.name
                                        from typchargr t
                                        	left join typchargrprop p on t.id=p.typchargr
                                        	inner join Prop pp on pp.id=p.prop and pp.proptype=6 and pp.reltyp=:parentRelTyp
                                        where t.typ=:typORrel
                                """;
                    } else {
                        typORrel = rtmORrcmRecord.getLong("reltypmemb");
                        sql = """
                                        select  distinct pp.id, pp.name
                                        from reltypchargr t
                                        	left join reltypchargrprop p on t.id=p.reltypchargr
                                        	inner join Prop pp on pp.id=p.prop and pp.proptype=6 and pp.reltyp=:parentRelTyp
                                        where t.relcls in (select id from RelCls where reltyp=:typORrel)
                                """;
                    }
                } else if (doit == FD_DimObjItemType_consts.relcls) {
                    StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select cls, relclsmemb from RelClsMember where id=:id",
                            Map.of("id", rtmORrcm));
                    if (rtmORrcmRecord.getLong("relclsmemb") == 0) {
                        clsORrel = rtmORrcmRecord.getLong("cls");
                        sql = """
                                        select distinct pp.id, pp.name
                                        from typchargr t
                                        	left join typchargrprop p on t.id=p.typchargr
                                        	inner join Prop pp on pp.id=p.prop and pp.proptype=6 and pp.reltyp=:parentRelTyp
                                        where t.typ in (select typ from Cls where id=:clsORrel)
                                        	and t.factorval in (select factorval from clsfactorval where cls=:clsORrel)
                                """;
                    } else {
                        clsORrel = rtmORrcmRecord.getLong("relclsmemb");
                        sql = """
                                        select  distinct pp.id, pp.name
                                        from reltypchargr t
                                        	left join reltypchargrprop p on t.id=p.reltypchargr
                                        	inner join Prop pp on pp.id=p.prop and pp.proptype=6 and pp.reltyp=:parentRelTyp
                                        where t.relcls=:clsORrel
                                """;
                    }
                }
            }

            if (linkType==11) {
                StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select typ, reltypmemb from RelTypMember where id=:id",
                        Map.of("id", rtmORrcm));

                if (doit == FD_DimObjItemType_consts.typ || doit == FD_DimObjItemType_consts.cls) {
                    if (rtmORrcmRecord.getLong("reltypmemb") == 0)
                        typORrel = rtmORrcmRecord.getLong("typ");
                    else
                        typORrel = rtmORrcmRecord.getLong("reltypmemb");

                    sql = """
                                select distinct id, name
                                from prop
                                where reltyp=:typORrel and id in (
                                	select p.prop from typchargr c
                                		left join typchargrprop p on c.id=p.typchargr and c.typ is not null
                                	where 0=0
                                )
                        """;
                }

                if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.relcls) {
                    if (rtmORrcmRecord.getLong("reltypmemb") == 0)
                        typORrel = rtmORrcmRecord.getLong("typ");
                    else
                        typORrel = rtmORrcmRecord.getLong("reltypmemb");

                    sql = """
                            select distinct id, name
                            from prop
                            where typ=:typORrel and id in (
                            	select p.prop from reltypchargr c
                            		left join reltypchargrprop p on c.id=p.reltypchargr and c.relcls is not null
                            	where 0=0
                            )
                    """;
                }
            }
        }
        if (parentDOIT==FD_DimObjItemType_consts.relcls || parentDOIT==FD_DimObjItemType_consts.urelcls) {
            if (parentDOIT == FD_DimObjItemType_consts.relcls)
                parentRelCls = recParent.getLong("relcls");
            else {
                long rtm = recParent.getLong("relclsMember");
                parentRelCls = mdb.loadQueryRecord("select relclsmemb from RelClsMember where id=:id",
                        Map.of("id", rtm)).getLong("relclsmemb");
            }

            if (linkType==10) {
                if (doit == FD_DimObjItemType_consts.reltyp) {
                    StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select typ, reltypmemb from RelTypMember where id=:id",
                            Map.of("id", rtmORrcm));

                    if (rtmORrcmRecord.getLong("reltypmemb") == 0) {
                        typORrel = rtmORrcmRecord.getLong("typ");
                        sql = """
                                select distinct pp.id, pp.name
                                from typchargr t
                                    left join typchargrprop p on t.id=p.typchargr
                                    inner join Prop pp on pp.id=p.prop and pp.proptype=6
                                            and pp.reltyp in (select reltyp from RelCls where id=:parentRelCls)
                                    left join PropVal pv on pp.id=pv.prop and pv.relcls=:parentRelCls
                                where t.typ=:typORrel
                        """;
                    } else {
                        typORrel = rtmORrcmRecord.getLong("reltypmemb");
                        sql = """
                                        select  distinct pp.id, pp.name
                                        from reltypchargr t
                                        	left join reltypchargrprop p on t.id=p.reltypchargr
                                        	inner join Prop pp on pp.id=p.prop and pp.proptype=6 and pp.reltyp in (select reltyp from RelCls where id=:parentRelCls)
                                        where t.relcls in (select id from RelCls where reltyp=:typORrel)
                                """;
                    }
                } else if (doit == FD_DimObjItemType_consts.relcls) {
                    StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select cls, relclsmemb from RelClsMember where id=:id",
                            Map.of("id", rtmORrcm));
                    if (rtmORrcmRecord.getLong("relclsmemb") == 0) {
                        clsORrel = rtmORrcmRecord.getLong("cls");
                        sql = """
                                        select distinct pp.id, pp.name
                                        from typchargr t
                                        	left join typchargrprop p on t.id=p.typchargr
                                        	inner join Prop pp on pp.id=p.prop and pp.proptype=6 and pp.reltyp in (select reltyp from RelCls where id=:parentRelCls)
                                        where t.typ in (select typ from Cls where id=:clsORrel)
                                        	and t.factorval in (select factorval from clsfactorval where cls=:clsORrel)
                                """;
                    } else {
                        clsORrel = rtmORrcmRecord.getLong("relclsmemb");
                        sql = """
                                        select  distinct pp.id, pp.name
                                        from reltypchargr t
                                        	left join reltypchargrprop p on t.id=p.reltypchargr
                                        	inner join Prop pp on pp.id=p.prop and pp.proptype=6 and pp.reltyp in (select reltyp from RelCls where id=:parentRelCls)
                                        where t.relcls = :clsORrel
                                """;
                    }
                }
            }

            if (linkType==11) {
                StoreRecord rtmORrcmRecord = mdb.loadQueryRecord("select cls, relclsmemb from RelClsMember where id=:id",
                        Map.of("id", rtmORrcm));

                if (doit == FD_DimObjItemType_consts.typ || doit == FD_DimObjItemType_consts.cls) {
                    if (rtmORrcmRecord.getLong("relclsmemb") == 0)
                        clsORrel = rtmORrcmRecord.getLong("cls");
                    else
                        clsORrel = rtmORrcmRecord.getLong("relclsmemb");

                    sql = """
                            select distinct pp.id, pp.name
                            from prop pp
                            	left join PropVal pv on pp.id=pv.prop
                            where pv.cls=:clsORrel  and pp.id in (
                            	select p.prop from typchargr c
                            		left join typchargrprop p on c.id=p.typchargr and c.typ is not null
                            	where 0=0
                            )
                        """;
                }

                if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.relcls) {
                    if (rtmORrcmRecord.getLong("relclsmemb") == 0)
                        clsORrel = rtmORrcmRecord.getLong("cls");
                    else
                        clsORrel = rtmORrcmRecord.getLong("relclsmemb");

                    sql = """
                            select distinct pp.id, pp.name
                            from prop pp
                            	left join PropVal pv on pp.id=pv.prop
                            where pv.cls=:clsORrel and pp.id in (
                            	select p.prop from reltypchargr c
                            		left join reltypchargrprop p on c.id=p.reltypchargr and c.relcls is not null
                            	where 0=0
                            )
                    """;
                }
            }
        }



        return mdb.loadQuery(sql, Map.of("parentTyp", parentTyp,"parentCls", parentCls, "parentRelTyp", parentRelTyp, "parentRelCls", parentRelCls,
                "typORrel", typORrel, "clsORrel", clsORrel));
    }

    public Store loadCompFor_11(long doit, long prop) throws Exception {
        String sql = "";
        if (doit==FD_DimObjItemType_consts.typ) {
            sql = """
                select distinct r.id, v.name
                from Typ r, TypVer v where r.id=v.ownerver and r.id in (
                    select c.typ from typchargr c
                        left join typchargrprop p on c.id=p.typchargr
                    where p.prop=:prop
                )
            """;
        }
        if (doit==FD_DimObjItemType_consts.cls) {
            sql = """
                    select distinct r.id, v.name
                    from Cls r, ClsVer v where r.id=v.ownerver and r.id in (
                    	select cfv.cls from typchargr c
                    		left join typchargrprop p on c.id=p.typchargr
                    		left join clsfactorval cfv on c.factorval=cfv.factorval
                    	where p.prop=:prop
                    )
            """;
        }
        if (doit==FD_DimObjItemType_consts.reltyp) {
            sql = """
                    select distinct r.id, v.name
                    from RelTyp r, RelTypVer v where r.id=v.ownerver and r.id in (
                    	select rc.reltyp from reltypchargr c
                    		left join reltypchargrprop p on c.id=p.reltypchargr
                    		left join RelCls rc on rc.id=c.relcls
                    	where p.prop=:prop
                    )
            """;
        }
        if (doit==FD_DimObjItemType_consts.relcls) {
            sql = """
                    select distinct r.id, v.name
                    from RelCls r, RelClsVer v where r.id=v.ownerver and r.id in (
                    	select c.relcls from reltypchargr c
                    		left join reltypchargrprop p on c.id=p.reltypchargr
                    	where p.prop=:prop
                    )
            """;
        }
        return mdb.loadQuery(sql, Map.of("prop", prop));
    }

    public Store loadProp_6(long memberType, long typORrel, long doit, long parent) throws Exception {
        String sql = "";
        long pt = 0;

        if (typORrel == 0) {
            StoreRecord recParent = loadRec(parent);
            long parentDOIT = recParent.getLong("dimObjItemType");
            if (parentDOIT == FD_DimObjItemType_consts.reltyp || parentDOIT == FD_DimObjItemType_consts.ureltyp) {
                if (parentDOIT == FD_DimObjItemType_consts.reltyp)
                    typORrel = recParent.getLong("reltyp");
                else {
                    long rtm = recParent.getLong("reltypMember");
                    typORrel = mdb.loadQueryRecord("select reltypmemb from RelTypMember where id=:id",
                            Map.of("id", rtm)).getLong("reltypmemb");
                }

            }

            if (parentDOIT == FD_DimObjItemType_consts.relcls || parentDOIT == FD_DimObjItemType_consts.urelcls) {
                if (parentDOIT == FD_DimObjItemType_consts.relcls)
                    typORrel = recParent.getLong("reltyp");
                else {
                    long rtm = recParent.getLong("reltypMember");
                    typORrel = mdb.loadQueryRecord("select reltypmemb from RelTypMember where id=:id",
                            Map.of("id", rtm)).getLong("reltypmemb");
                }

            }
        }

        if (doit == FD_DimObjItemType_consts.typ || doit == FD_DimObjItemType_consts.cls ||
                doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.relcls) {
            pt = 5;
            if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.relcls)
                pt = 6;

            if (memberType == FD_MemberType_consts.typ) {
                sql = """
                            select distinct  pp.id, pp.name
                            from typchargr c
                            left join typchargrprop p on c.id=p.typchargr
                            left join Prop pp on pp.id=p.prop
                            where c.typ=:typORrel and pp.proptype=:pt
                        """;
            } else {
                sql = """
                            select distinct  pp.id, pp.name
                            from reltypchargr c
                            left join reltypchargrprop p on c.id=p.reltypchargr
                            left join Prop pp on pp.id=p.prop
                            where c.relcls in (select id from RelCls where reltyp=:typORrel) and pp.proptype=:pt
                        """;
            }
        }


        return mdb.loadQuery(sql, Map.of("typORrel", typORrel, "pt", pt));
    }

    public Store loadProp(long doit, long parent, long linkType) throws Exception {
        StoreRecord recParent = loadRec(parent);
        long parentDOIT = recParent.getLong("dimObjItemType");
        long typ = 0;
        long cls = 0;
        long reltyp = 0;
        long relcls = 0;
        long pt = 0;
        String sql = "";

        if (linkType == 5) {
            //1
            if (parentDOIT == FD_DimObjItemType_consts.typ || parentDOIT == FD_DimObjItemType_consts.utyp) {

                if (parentDOIT == FD_DimObjItemType_consts.typ)
                    typ = recParent.getLong("typ");
                else {
                    long rtm = recParent.getLong("reltypMember");
                    typ = mdb.loadQueryRecord("select typ from RelTypMember where id=:id",
                            Map.of("id", rtm)).getLong("typ");
                }
                //doit:
                pt = 5;
                if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.relcls)
                    pt = 6;

                sql = """
                            select distinct pp.id, pp.name
                            from typchargr c
                                left join typchargrprop p on c.id=p.typchargr
                                left join Prop pp on pp.id=p.prop
                            where c.typ=:typ and (pp.proptype=:pt)
                        """;
            }
            //2 cls...
            if (parentDOIT == FD_DimObjItemType_consts.cls || parentDOIT == FD_DimObjItemType_consts.ucls) {
                if (parentDOIT == FD_DimObjItemType_consts.cls)
                    cls = recParent.getLong("cls");
                else {
                    long rtm = recParent.getLong("relclsMember");
                    cls = mdb.loadQueryRecord("select cls from RelClsMember where id=:id",
                            Map.of("id", rtm)).getLong("cls");
                }
                //doit:
                pt = 5;
                if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.relcls)
                    pt = 6;

                sql = """
                                select distinct pp.id, pp.name
                                from typchargr c
                                    left join typchargrprop p on c.id=p.typchargr
                                    left join Prop pp on pp.id=p.prop
                                where c.typ in (select typ from cls where id=:cls) and pp.proptype=:pt
                                	and c.factorval in (select factorval from clsfactorval where cls=:cls)
                        """;
            }
            //3 reltyp...
            if (parentDOIT == FD_DimObjItemType_consts.reltyp || parentDOIT == FD_DimObjItemType_consts.ureltyp) {

                if (parentDOIT == FD_DimObjItemType_consts.reltyp)
                    reltyp = recParent.getLong("reltyp");
                else {
                    long rtm = recParent.getLong("reltypMember");
                    reltyp = mdb.loadQueryRecord("select reltypmember from RelTypMember where id=:id",
                            Map.of("id", rtm)).getLong("reltypmember");
                }
                //doit:
                pt = 5;
                if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.relcls)
                    pt = 6;

                sql = """
                            select distinct pp.id, pp.name
                            from reltypchargr c
                                left join reltypchargrprop p on c.id=p.reltypchargr
                                left join Prop pp on pp.id=p.prop
                            where c.relcls in (select id from relcls where reltyp=:reltyp) and pp.proptype=:pt
                        """;
            }
            //4 relcls...
            if (parentDOIT == FD_DimObjItemType_consts.relcls || parentDOIT == FD_DimObjItemType_consts.urelcls) {

                if (parentDOIT == FD_DimObjItemType_consts.relcls)
                    relcls = recParent.getLong("relcls");
                else {
                    long rtm = recParent.getLong("relclsMember");
                    relcls = mdb.loadQueryRecord("select relclsmember from RelClsMember where id=:id",
                            Map.of("id", rtm)).getLong("relclsmember");
                }
                //doit:
                pt = 5;
                if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.relcls)
                    pt = 6;

                sql = """
                            select distinct pp.id, pp.name
                            from reltypchargr c
                                left join reltypchargrprop p on c.id=p.reltypchargr
                                left join Prop pp on pp.id=p.prop
                            where c.relcls=:relcls and pp.proptype=:pt
                        """;
            }
        } else if (linkType == 7) {
            if (parentDOIT == FD_DimObjItemType_consts.typ || parentDOIT == FD_DimObjItemType_consts.utyp) {

                if (parentDOIT == FD_DimObjItemType_consts.typ)
                    typ = recParent.getLong("typ");
                else {
                    long rtm = recParent.getLong("reltypMember");
                    typ = mdb.loadQueryRecord("select typ from RelTypMember where id=:id",
                            Map.of("id", rtm)).getLong("typ");
                }

                sql = """
                            select distinct pp.id, pp.name, pp.proptype,
                            case when pp.proptype=5 then pp.typ else pp.reltyp end as typORrel
                            from typchargr c
                                left join typchargrprop p on c.id=p.typchargr
                                inner join Prop pp on pp.id=p.prop and p.prop is not null
                            where c.typ=:typ and (pp.proptype=5 or pp.proptype=6)
                        """;
            }

            if (parentDOIT == FD_DimObjItemType_consts.cls || parentDOIT == FD_DimObjItemType_consts.ucls) {
                if (parentDOIT == FD_DimObjItemType_consts.cls)
                    cls = recParent.getLong("cls");
                else {
                    long rtm = recParent.getLong("relclsMember");
                    cls = mdb.loadQueryRecord("select cls from RelClsMember where id=:id",
                            Map.of("id", rtm)).getLong("cls");
                }

                sql = """
                            select distinct pp.id, pp.name, pp.proptype,
                                case when pp.proptype=5 then pp.typ else pp.reltyp end as typORrel
                            from typchargr c
                                left join typchargrprop p on c.id=p.typchargr
                                inner join Prop pp on pp.id=p.prop and (pp.proptype=5 or pp.proptype=6)
                            where c.typ in (select typ from Cls where id=:cls)
                                and c.factorval in (select factorval from clsfactorval where cls=:cls)
                        """;
            }

            if (parentDOIT == FD_DimObjItemType_consts.reltyp || parentDOIT == FD_DimObjItemType_consts.ureltyp) {
                if (parentDOIT == FD_DimObjItemType_consts.reltyp)
                    reltyp = recParent.getLong("reltyp");
                else {
                    long rtm = recParent.getLong("reltypMember");
                    reltyp = mdb.loadQueryRecord("select reltypmemb from RelTypMember where id=:id",
                            Map.of("id", rtm)).getLong("reltypmemb");
                }

                sql = """
                            select distinct pp.id, pp.name, pp.proptype,
                            case when pp.proptype=5 then pp.typ else pp.reltyp end as typORrel
                            from reltypchargr c
                            left join reltypchargrprop p on c.id=p.reltypchargr
                            inner join Prop pp on pp.id=p.prop
                            where c.relcls in (select id from RelCls where reltyp=:reltyp) and (pp.proptype=5 or pp.proptype=6)
                        """;
            }
            if (parentDOIT == FD_DimObjItemType_consts.relcls || parentDOIT == FD_DimObjItemType_consts.urelcls) {
                if (parentDOIT == FD_DimObjItemType_consts.relcls)
                    relcls = recParent.getLong("relcls");
                else {
                    long rtm = recParent.getLong("relclsMember");
                    relcls = mdb.loadQueryRecord("select relclsmemb from RelClsMember where id=:id",
                            Map.of("id", rtm)).getLong("relclsmemb");
                }

                sql = """
                            select distinct pp.id, pp.name, pp.proptype,
                                case when pp.proptype=5 then pp.typ else pp.reltyp end as typORrel
                            from reltypchargr c
                                left join reltypchargrprop p on c.id=p.reltypchargr
                                inner join Prop pp on pp.id=p.prop
                            where c.relcls=:relcls and (pp.proptype=5 or pp.proptype=6)
                        """;
            }
        } else if (linkType == 9) {
            if (parentDOIT == FD_DimObjItemType_consts.typ || parentDOIT == FD_DimObjItemType_consts.utyp) {

                if (parentDOIT == FD_DimObjItemType_consts.typ)
                    typ = recParent.getLong("typ");
                else {
                    long rtm = recParent.getLong("reltypMember");
                    typ = mdb.loadQueryRecord("select typ from RelTypMember where id=:id",
                            Map.of("id", rtm)).getLong("typ");
                }
                sql = """
                            select id, name from prop where typ=:typ and proptype=5
                        """;
            }
            if (parentDOIT == FD_DimObjItemType_consts.cls || parentDOIT == FD_DimObjItemType_consts.ucls) {

                if (parentDOIT == FD_DimObjItemType_consts.cls)
                    cls = recParent.getLong("cls");
                else {
                    long rtm = recParent.getLong("relclsMember");
                    cls = mdb.loadQueryRecord("select cls from RelClsMember where id=:id",
                            Map.of("id", rtm)).getLong("cls");
                }
                sql = """
                            select p.id, p.name from PropVal pv, Prop p where pv.cls=:cls and pv.prop= p.id
                        """;
            }
            if (parentDOIT == FD_DimObjItemType_consts.reltyp || parentDOIT == FD_DimObjItemType_consts.ureltyp) {

                if (parentDOIT == FD_DimObjItemType_consts.reltyp)
                    reltyp = recParent.getLong("reltyp");
                else {
                    long rtm = recParent.getLong("reltypMember");
                    reltyp = mdb.loadQueryRecord("select reltypmemb from RelTypMember where id=:id",
                            Map.of("id", rtm)).getLong("reltypmemb");
                }
                sql = """
                            select id, name from prop where reltyp=:reltyp
                        """;
            }
            if (parentDOIT == FD_DimObjItemType_consts.relcls || parentDOIT == FD_DimObjItemType_consts.urelcls) {

                if (parentDOIT == FD_DimObjItemType_consts.relcls)
                    relcls = recParent.getLong("relcls");
                else {
                    long rtm = recParent.getLong("relclsMember");
                    relcls = mdb.loadQueryRecord("select relclsmemb from RelClsMember where id=:id",
                            Map.of("id", rtm)).getLong("relclsmemb");
                }
                sql = """
                            select p.id, p.name from propVal pv	left join Prop p on p.id=pv.prop where pv.relcls=:relcls
                        """;
            }

        }


        return mdb.loadQuery(sql, Map.of("typ", typ, "cls", cls,
                "reltyp", reltyp, "relcls", relcls, "pt", pt));
    }


    public Store loadValueOfProp(long doit, long prop, long parent, long linkType) throws Exception {
        StoreRecord recParent = loadRec(parent);
        long parentDOIT = recParent.getLong("dimObjItemType");
        String sql = "";

        if (linkType == 5) {
            if (doit == FD_DimObjItemType_consts.typ || doit == FD_DimObjItemType_consts.utyp) {
                sql = """
                            select t.id, v.name from Typ t, TypVer v
                            where t.id=v.ownerver and t.id in (select typ from Prop where id=:prop)
                        """;
            }
            if (doit == FD_DimObjItemType_consts.cls || doit == FD_DimObjItemType_consts.ucls) {
                sql = """
                            select distinct c.id, v.name from Cls c, ClsVer v
                            where c.id=v.ownerver and c.id in (
                                    select cls from PropVal where prop=:prop and cls is not null
                                )
                        """;
            }
            if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.ureltyp) {
                sql = """
                            select t.id, v.name from RelTyp t, RelTypVer v
                            where t.id=v.ownerver and t.id in (select reltyp from Prop where id=:prop)
                        """;
            }
            if (doit == FD_DimObjItemType_consts.relcls || doit == FD_DimObjItemType_consts.urelcls) {
                sql = """
                            select distinct c.id, v.name from RelCls c, RelClsVer v
                            where c.id=v.ownerver and c.id in (
                                    select relcls from PropVal where prop=:prop and relcls is not null
                                )
                        """;
            }

        }
        if (linkType == 6) {
            if (doit == FD_DimObjItemType_consts.typ || doit == FD_DimObjItemType_consts.utyp) {
                sql = """
                            select t.id, v.name from Typ t, TypVer v
                            where t.id=v.ownerver and t.id in (select typ from Prop where id=:prop)
                        """;
            } else if (doit == FD_DimObjItemType_consts.cls || doit == FD_DimObjItemType_consts.ucls) {
                sql = """
                        select t.id, v.name from Cls t, ClsVer v where t.id=v.ownerver and
                            t.id in (select cls from PropVal where prop=:prop and cls is not null)
                        """;
            } else if (doit == FD_DimObjItemType_consts.reltyp || doit == FD_DimObjItemType_consts.ureltyp) {
                sql = """
                            select t.id, v.name
                            from RelTyp t, RelTypVer v
                            where t.id=v.ownerver and t.id in (select reltyp from Prop where id=:prop)
                        """;
            } else if (doit == FD_DimObjItemType_consts.relcls || doit == FD_DimObjItemType_consts.urelcls) {
                sql = """
                            select t.id, v.name from RelCls t, RelClsVer v where t.id=v.ownerver and
                                t.id in (select relcls from PropVal where prop=:prop and relcls is not null)
                        """;
            }
        }
        if (linkType == 9) {
            // !!! От родителя не зависит
            if (parentDOIT == FD_DimObjItemType_consts.typ || parentDOIT == FD_DimObjItemType_consts.utyp ||
                    parentDOIT == FD_DimObjItemType_consts.cls || parentDOIT == FD_DimObjItemType_consts.ucls ||
                    parentDOIT == FD_DimObjItemType_consts.reltyp || parentDOIT == FD_DimObjItemType_consts.ureltyp ||
                    parentDOIT == FD_DimObjItemType_consts.relcls || parentDOIT == FD_DimObjItemType_consts.urelcls) {
                if (doit == FD_DimObjItemType_consts.typ) {
                    sql = """
                                select t.id, v.name
                                from Typ t, TypVer v where t.id=v.ownerver and v.lastver=1
                                    and t.id in (
                                        select t.typ
                                        from typchargr t
                                            left join typchargrprop p on t.id=p.typchargr
                                        where p.prop=:prop
                                    )
                            """;
                }
                if (doit == FD_DimObjItemType_consts.cls) {
                    sql = """
                                select distinct c.id, v.name
                                from cls c
                                    left join clsver v on c.id=v.ownerver and v.lastver=1
                                    left join clsfactorval cfv on c.id=cfv.cls
                                where c.typ in (
                                        select t.typ
                                        from typchargr t
                                            left join typchargrprop p on t.id=p.typchargr
                                        where p.prop=:prop
                                    ) and cfv.factorval in (
                                        select t.factorval
                                        from typchargr t
                                            left join typchargrprop p on t.id=p.typchargr
                                        where p.prop=:prop
                                    )
                            """;
                }
                if (doit == FD_DimObjItemType_consts.reltyp) {
                    sql = """
                                select distinct t.id, v.name
                                from RelTyp t, RelTypVer v where t.id=v.ownerver and v.lastver=1
                                    and t.id in (
                                        select rc.reltyp
                                        from reltypchargr t
                                            left join reltypchargrprop p on t.id=p.reltypchargr
                                            left join RelCls rc on t.relcls=rc.id
                                        where p.prop=:prop
                                    )
                            """;
                }
                if (doit == FD_DimObjItemType_consts.relcls) {
                    sql = """
                                select distinct t.id, v.name
                                from RelCls t, RelClsVer v where t.id=v.ownerver and v.lastver=1
                                    and t.id in (
                                        select t.relcls
                                        from reltypchargr t
                                            left join reltypchargrprop p on t.id=p.reltypchargr
                                        where p.prop=:prop
                                    )
                            """;
                }
            }
        }


        return mdb.loadQuery(sql, Map.of("prop", prop));

    }

    public Store loadValueOfProp_7(long doit, long prop, long rtmORrcm, long typORrel, long linkType) throws Exception {
        //StoreRecord recParent = loadRec(parent);
        //long parentDOIT = recParent.getLong("dimObjItemType");

        long propType = mdb.loadQuery("select propType from Prop where id=:p", Map.of("p", prop))
                .get(0).getLong("propType");

        if (typORrel == 0) {
            if (doit == FD_DimObjItemType_consts.reltyp) {
                StoreRecord recRTM = mdb.loadQuery("select typ, reltypmemb from RelTypMember where id=:id",
                        Map.of("id", rtmORrcm)).get(0);
                typORrel = recRTM.getLong("typ") == 0 ? recRTM.getLong("reltypmemb") : recRTM.getLong("typ");
            }
            if (doit == FD_DimObjItemType_consts.relcls) {
                StoreRecord recRCM = mdb.loadQuery("""
                            select cls, relclsmemb, c.typ, rc.reltyp
                            from RelClsMember rcm
                                left join Cls c on rcm.cls=c.id
                                left join RelCls rc on rcm.relclsmemb=rc.id
                            where rcm.id=:id
                        """, Map.of("id", rtmORrcm)).get(0);

                typORrel = recRCM.getLong("typ") == 0 ? recRCM.getLong("reltyp") : recRCM.getLong("typ");
            }

        }


        String sql = "";

        if (linkType == 7) {

            if (doit == FD_DimObjItemType_consts.reltyp) {
                if (propType == 5) {
                    sql = """
                                select r.id, rv.name, rtm.id as rtm, 5 as proptype
                                from reltypmember rtm
                                    left join RelTyp r on r.id=rtm.reltyp
                                    left join RelTypVer rv on r.id=rv.ownerver and rv.lastver=1
                                where rtm.typ=:typORrel
                            """;
                }
                if (propType == 6) {
                    sql = """
                                select r.id, rv.name, rtm.id as rtm, 6 as proptype
                                from reltypmember rtm
                                    left join RelTyp r on r.id=rtm.reltyp
                                    left join RelTypVer rv on r.id=rv.ownerver and rv.lastver=1
                                where rtm.reltypmemb = :typORrel
                            """;
                }
            }
            if (doit == FD_DimObjItemType_consts.relcls) {
                if (propType == 5) {
                    sql = """
                                select r.id, rv.name, rcm.id as rcm, 5 as proptype
                                from relclsmember rcm
                                    left join RelCls r on r.id=rcm.relcls
                                    left join RelClsVer rv on r.id=rv.ownerver and rv.lastver=1
                                where rcm.cls in (select id from Cls where typ=:typORrel)
                            """;
                }
                if (propType == 6) {
                    sql = """
                                select r.id, rv.name, rcm.id as rcm, 6 as proptype
                                from relclsmember rcm
                                    left join RelCls r on r.id=rcm.relcls
                                    left join RelClsVer rv on r.id=rv.ownerver and rv.lastver=1
                                where rcm.relclsmemb in (select id from RelCls where reltyp=:typORrel)
                            """;
                }
            }

        }


        return mdb.loadQuery(sql, Map.of("typORrel", typORrel));

    }


    //*********************************************************

    private void getStoreForTyp(Store st, StoreRecord r, String codParentItem, long isRelParentItem,
                                long idParentItem, long typParentItem, long clsParentItem, String dt) throws Exception {
        //1 отсутствует
        //5 дочерний компонент - значения одного или нескольких свойств родительского компонента
        //6 дочерний компонент - значения одного или нескольких свойств участников родительского компонента
        //9 родительский компонент - значения одного или нескольких свойств дочернего компонента
        //11 участник(и) родительского компонента - значения одного или нескольких свойств дочернего компонента
        long lev = r.getLong("lev");
        long linkType = r.getLong("linkType");
        if (linkType == 1) {
            if (lev == 0) {
                if (r.getLong("cls") > 0) {
                    mdb.loadQuery(st, """
                               select o.cod as id,
                               case when v.objparent is null then :cod else o2.cod end as parent,
                               v.name, o.cod, v.dbeg, v.dend
                               from Obj o
                                left join ObjVer v on o.id=v.ownerver
                                left join Obj o2 on o2.id=v.objParent
                               where o.cls=:cls and :dt between v.dbeg and v.dend
                            """, Map.of("cls", r.getLong("cls"), "cod", codParentItem, "dt", dt));
                } else {
                    mdb.loadQuery(st, """
                               select o.cod as id,
                               case when v.objparent is null then :cod else o2.cod end as parent,
                               v.name, o.cod, v.dbeg, v.dend
                               from Obj o
                                left join ObjVer v on o.id=v.ownerver
                                left join Obj o2 on o2.id=v.objParent
                               where o.cls in (select id from Cls where typ=:typ) and :dt between v.dbeg and v.dend
                            """, Map.of("typ", r.getLong("typ"), "cod", codParentItem, "dt", dt));
                }
            } else if (lev == 1) {
                if (r.getLong("cls") > 0) {
                    mdb.loadQuery(st, """
                               select o.cod as id, :cod as parent, v.name, o.cod, v.dbeg, v.dend
                               from Obj o
                                left join ObjVer v on o.id=v.ownerver
                               where o.cls=:cls and :dt between v.dbeg and v.dend and v.objParent is null
                            """, Map.of("cls", r.getLong("cls"), "cod", codParentItem, "dt", dt));
                } else {
                    mdb.loadQuery(st, """
                               select o.cod as id, :cod as parent, v.name, o.cod, v.dbeg, v.dend
                               from Obj o
                                left join ObjVer v on o.id=v.ownerver
                               where o.cls in (select id from Cls where typ=:typ)
                                and :dt between v.dbeg and v.dend and v.objParent is null
                            """, Map.of("typ", r.getLong("typ"), "cod", codParentItem, "dt", dt));
                }
            } else if (lev == 2) {
                if (r.getLong("cls") > 0) {
                    mdb.loadQuery(st, """
                               with t as (
                                   select o.id
                                   from Obj o
                                    left join ObjVer v on o.id=v.ownerver
                                   where o.cls=:cls and :dt between v.dbeg and v.dend and v.objParent is null
                               )
                               select o.cod as id, :cod as parent, v.name, o.cod, v.dbeg, v.dend
                               from Obj o
                                left join ObjVer v on o.id=v.ownerver
                               where o.cls=:cls and :dt between v.dbeg and v.dend and v.objParent in (select id from t)
                            """, Map.of("cls", r.getLong("cls"), "cod", codParentItem, "dt", dt));
                } else {
                    mdb.loadQuery(st, """
                               with t as (
                                   select o.id
                                   from Obj o
                                    left join ObjVer v on o.id=v.ownerver
                                   where o.cls in (select id from Cls where typ=:typ)
                                    and :dt between v.dbeg and v.dend and v.objParent is null
                               )
                               select o.cod as id, :cod as parent, v.name, o.cod, v.dbeg, v.dend
                               from Obj o
                                left join ObjVer v on o.id=v.ownerver
                               where o.cls in (select id from Cls where typ=:typ)
                                and :dt between v.dbeg and v.dend and v.objParent in (select id from t)
                            """, Map.of("typ", r.getLong("typ"), "cod", codParentItem, "dt", dt));
                }

            } else {
                Store stTmp = mdb.createStore("DimObj.comp");
                if (r.getLong("cls") > 0) {
                    mdb.loadQuery(stTmp, """
                               select o.cod as id, :cod as parent, v.name, o.cod, v.dbeg, v.dend
                               from Obj o
                                left join ObjVer v on o.id=v.ownerver
                               where o.cls=:cls and :dt between v.dbeg and v.dend
                            """, Map.of("cls", r.getLong("cls"), "cod", codParentItem, "dt", dt));
                } else {
                    mdb.loadQuery(stTmp, """
                               select o.cod as id, :cod as parent, v.name, o.cod, v.dbeg, v.dend
                               from Obj o
                                left join ObjVer v on o.id=v.ownerver
                               where o.cls in (select id from Cls where typ=:typ)
                                and :dt between v.dbeg and v.dend
                            """, Map.of("typ", r.getLong("typ"), "cod", codParentItem, "dt", dt));
                }
                //

                DataTreeNode dtn = UtData.createTreeIdParent(stTmp, "id", "parent");
                UtData.scanTree(dtn, false, new ITreeNodeVisitor() {
                    @Override
                    public void visitNode(DataTreeNode node) {
                        if (node.getLevel() == r.getLong("lev")) {
                            st.add(node.getRecord());
                        }
                    }
                });

            }

        } else {
            Store stProps = mdb.createStore("DimObjItemProp.source");
            mdb.loadQuery(stProps, """
                        select d.*, f.id as idFlatTable, f.nameTable as nameFlatTable,
                            p.statusFactor, p.providerTyp, ps.factorVal as defaultStatus, pp.obj as defaultProvider
                        from DimObjItemProp d
                            left join Prop p on d.prop=p.id
                            left join FlatTableProp ftp on ftp.prop=d.prop
                            left join FlatTable f on f.id=ftp.flatTable
                            left join FlatTableOwn fto on f.id=fto.flatTable and fto.typ=:typ
                            left join PropStatus ps on ps.prop=p.id and ps.isDefault=1
                            left join PropProvider pp on pp.prop=p.id and pp.isDefault=1
                        where d.dimObjItem=:doi
                    """, Map.of("doi", r.getLong("id"), "typ", typParentItem));

            if (stProps.size() == 0) {
                throw new XError("Не найдено свойства родительского компонента");
            }

            if (linkType == 5) {
                long isObj = 1;
                if (isRelParentItem == 1) isObj = 0;

                for (StoreRecord rec : stProps) {
                    if (rec.getLong("idFlatTable") == 0) {    // from DataPropVal
                        /// Продолжение здесь!!!!!
                        Store stVal = mdb.loadQuery("""
                                    select d.*, pv.obj from DataProp d, DataPropVal v, PropVal pv
                                    where d.id=v.dataProp and v.propval=pv.id and d.isObj=:isObj and d.own=:own
                                        and d.prop=:prop and :dt between v.dbeg and v.dend
                                """, Map.of("isObj", isObj, "own", idParentItem, "prop", rec.getLong("prop")));


                    } else {    // from FlatTable
                        int o = 0;
                    }

                }


            } else if (linkType == 6) {
                int o = 0;
            } else if (linkType == 9) {
                int o = 0;
            } else if (linkType == 11) {
                int o = 0;
            }

        }

    }

    private void getStoreForRelTyp(Store st, StoreRecord r, String cod, String dt) throws Exception {
        System.out.println(dt);
    }

    private void getStoreForTypMember(Store st, StoreRecord r, String cod, String dt) throws Exception {
        System.out.println(dt);

    }

    private void getStoreForRelTypMember(Store st, StoreRecord r, String cod, String dt) throws Exception {
        System.out.println(dt);

    }


    public Store loadTreeForView(Map<String, Object> params) throws Exception {
        String dt = UtCnv.toString(params.get("currDate"));
        long dimobj = UtCnv.toLong(params.get("dimObj"));
        String codParentItem = null;
        long idParentItem = 0;
        long isRelParentItem = -1;
        long typParentItem = 0;
        long clsParentItem = 0;
        long reltypParentItem = 0;

        if (params.containsKey("id")) {
            codParentItem = UtCnv.toString(params.get("id"));   //cod Obj or cod RelObj

            Store st = mdb.loadQuery("select * from syscod where cod like :cod", Map.of("cod", codParentItem));
            if (st.size() == 0) {
                throw new XError("Экземпляр сущности удален");
            } else {
                idParentItem = st.get(0).getLong("linkid");
                if (st.get(0).getLong("linktype") == 10) {
                    isRelParentItem = 0;
                    Store stTmp = mdb.loadQuery("""
                                select o.cls, c.typ from Obj o left join Cls c on o.cls=c.id
                                where o.id=:id
                            """, Map.of("id", idParentItem));
                    typParentItem = stTmp.get(0).getLong("typ");
                    clsParentItem = stTmp.get(0).getLong("cls");
                } else {
                    isRelParentItem = 1;
                    Store stTmp = mdb.loadQuery("""
                                select reltyp from RelObj where id=:id
                            """, Map.of("id", idParentItem));
                    reltypParentItem = stTmp.get(0).getLong("reltyp");
                }
            }

        }
        long dimObjItem = UtCnv.toLong(params.get("dimObjItem"));


        Store stRes = mdb.createStore("DimObj.comp");
        Store stTmp = mdb.createStore("DimObjItem");

        if (dimObjItem == 0) {    // 1 level
            mdb.loadQuery(stTmp, """
                        select * from DimObjItem where dimObj=:do and parent is null
                    """, Map.of("do", dimobj));

        } else { // child level
            mdb.loadQuery(stTmp, """
                        select * from DimObjItem where dimObj=:do and parent = :doi
                    """, Map.of("do", dimobj, "doi", dimObjItem));

        }

        for (StoreRecord r : stTmp) {
            Store resTmp = mdb.createStore("DimObj.comp");
            if (r.getLong("dimObjItemType") == FD_DimObjItemType_consts.typ ||
                    r.getLong("dimObjItemType") == FD_DimObjItemType_consts.cls) {
                getStoreForTyp(resTmp, r, codParentItem, isRelParentItem, idParentItem, typParentItem, clsParentItem, dt);

            } else if (r.getLong("dimObjItemType") == FD_DimObjItemType_consts.reltyp) {
                getStoreForRelTyp(resTmp, r, codParentItem, dt);

            } else if (r.getLong("dimObjItemType") == FD_DimObjItemType_consts.utyp ||
                    r.getLong("dimObjItemType") == FD_DimObjItemType_consts.ucls) {
                getStoreForTypMember(resTmp, r, codParentItem, dt);

            } else if (r.getLong("dimObjItemType") == FD_DimObjItemType_consts.ureltyp) {
                getStoreForRelTypMember(resTmp, r, codParentItem, dt);

            }
            stRes.add(resTmp);
        }


        //DataTreeNode dtn = UtData.createTreeIdParent(st, "id", "parent");

/*
        UtData.scanTree(dtn, false, new ITreeNodeVisitor() {
            @Override
            public void visitNode(DataTreeNode node) {
                System.out.println("level: "+node.getLevel());
                mdb.outTable(node.getRecord());
            }
        });
*/


        return stRes;
    }

}
