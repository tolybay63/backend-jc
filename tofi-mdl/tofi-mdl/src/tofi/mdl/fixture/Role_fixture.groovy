package tofi.mdl.fixture


import jandcode.core.dbm.fixture.BaseFixtureBuilder

class Role_fixture extends BaseFixtureBuilder {

    @Override
    protected void onBuild() {
        def tab = fx.table("role")
        def tabSC = fx.table("syscod")

        tab.add(id: 1, accessLevel: 1, cod: "_R_1", name: "Роль 1", fullName: "Роль 1")
        tab.add(id: 2, accessLevel: 1, cod: "_R_2", name: "Роль 2", fullName: "Роль 2")


        //CodSys
        long id = 38
        for (long i = 1; i < 3; i++) {
            tabSC.add(id: id++, cod: "_R_${i}", linkType: 7, linkId: i)
        }

    }

}
