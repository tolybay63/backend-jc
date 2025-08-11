package tofi.mdl.fixture


import jandcode.core.dbm.fixture.BaseFixtureBuilder

class Measure_fixture extends BaseFixtureBuilder {

    @Override
    protected void onBuild() {
        def tab = fx.table("measure")
        def tabSC = fx.table("syscod")

        tab.add(id: 1, accessLevel: 1, cod: "_M_1", kFromBase: "1", name: "тг", fullName: "тенге")
        tab.add(id: 2, accessLevel: 1, cod: "_M_2", kFromBase: "1", name: "шт.", fullName: "штук")
        tab.add(id: 3, parent: "1", accessLevel: 1, cod: "_M_3", kFromBase: "0,001", name: "тыс.тг.", fullName: "тысяча тенге")
        tab.add(id: 4, parent: "2", accessLevel: 1, cod: "_M_4", kFromBase: "0,01", name: "сто штук", fullName: "сто штук")

        //CodSys
        long id = 1
        for (long i = 1; i < 5; i++) {
            tabSC.add(id: id++, cod: "_M_${i}", linkType: 1, linkId: i)
        }

    }

}
