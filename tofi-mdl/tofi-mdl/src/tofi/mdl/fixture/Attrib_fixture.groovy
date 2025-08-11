package tofi.mdl.fixture


import jandcode.core.dbm.fixture.BaseFixtureBuilder

class Attrib_fixture extends BaseFixtureBuilder {

    @Override
    protected void onBuild() {
        def tab = fx.table("attrib")
        def tabChar = fx.table("attribchar")
        def tabSC = fx.table("syscod")

        tab.add(id: 1, accessLevel: 1, cod: "_A_1", attribvaltype: 1, name: "строка", fullName: "строка")
        tab.add(id: 2, accessLevel: 1, cod: "_A_2", attribvaltype: 2, name: "ИНН", fullName: "ИНН")
        tab.add(id: 3, accessLevel: 1, cod: "_A_3", attribvaltype: 9, name: "файл", fullName: "файл")

        tabChar.add(id: 1, attrib: 2, maskreg: "############")
        tabChar.add(id: 2, attrib: 3, fileext: "*.png")


        //CodSys
        long id = 5
        for (long i = 1; i < 4; i++) {
            tabSC.add(id: id++, cod: "_A_${i}", linkType: 2, linkId: i)
        }

    }

}
