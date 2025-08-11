package tofi.mdl.fixture


import jandcode.core.dbm.fixture.BaseFixtureBuilder

class Meter_fixture extends BaseFixtureBuilder {

    @Override
    protected void onBuild() {
        def tab = fx.table("meter")
        def tabfactor = fx.table("meterfactor")
        def tabSC = fx.table("syscod")

        tab.add(id: 1, accessLevel: 1, cod: "_I_1", measure: 1, meterstruct: 2, meterdeterm: 1, distributionlaw: 1, metertypebyrate: 1, metertypebyperiod: 1, metertypebymember: 1, meterbehavior: 1, name: "meter soft", fullname: "meter soft")
        tab.add(id: 2, accessLevel: 1, cod: "_I_2", measure: 2, meterstruct: 1, meterdeterm: 1, distributionlaw: 1, metertypebyrate: 1, metertypebyperiod: 1, metertypebymember: 1, meterbehavior: 1, name: "meter hard", fullname: "meter hard")

        tabfactor.add(id: 1, meter: 1, factor: 1, orddim: 1, ordfactorindim: 1)
        tabfactor.add(id: 2, meter: 1, factor: 2, orddim: 2, ordfactorindim: 1)
        tabfactor.add(id: 3, meter: 1, factor: 3, orddim: 3, ordfactorindim: 1)


        //CodSys
        long id = 35
        for (long i = 1; i < 3; i++) {
            tabSC.add(id: id++, cod: "_I_${i}", linkType: 5, linkId: i)
        }

    }

}
