package tofi.mdl.fixture


import jandcode.core.dbm.fixture.BaseFixtureBuilder

class Typ_fixture extends BaseFixtureBuilder {

    @Override
    protected void onBuild() {
        def tab = fx.table("typ")
        def tabVer = fx.table("typVer")
        def tabSC = fx.table("syscod")

        def tabRole = fx.table("typRole")
        def tabRoleVer = fx.table("TypRoleLifeInterval")


        tab.add(id: 1, accessLevel: 1, cod: "_T_1", isopenness: 1, typcategory: 1, ord: 1)
        tabVer.add(id: 1, ownerver: 1, name: "Родительский тип", fullName: "Родительский тип", dbeg: '1800-01-01', dend: '3333-12-31', lastver: 1)

        tab.add(id: 2, accessLevel: 1, cod: "_T_2", parent: 1L, isopenness: 1, typcategory: 1, ord: 2)
        tabVer.add(id: 3, ownerver: 2, name: "Дочерный тип", fullName: "Дочерный тип", dbeg: '1800-01-01', dend: '3333-12-31', lastver: 1)

        tabRole.add(id: 1, typ: 1, role: 1, cmt: "Role 1")
        tabRoleVer.add(id: 1, typRole: 1, dbeg: '1800-01-01', dend: '3333-12-31', cmt: "Role 1")

        tabRole.add(id: 2, typ: 2, role: 2, cmt: "Role 2")
        tabRoleVer.add(id: 2, typRole: 2, dbeg: '1800-01-01', dend: '3333-12-31', cmt: "Role 2")

        //CodSys
        long id = 40
        for (long i = 1; i < 3; i++) {
            tabSC.add(id: id++, cod: "_T_${i}", linkType: 8, linkId: i)
        }

    }

}
