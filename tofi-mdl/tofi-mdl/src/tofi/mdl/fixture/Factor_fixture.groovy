package tofi.mdl.fixture


import jandcode.core.dbm.fixture.BaseFixtureBuilder

class Factor_fixture extends BaseFixtureBuilder {

    @Override
    protected void onBuild() {
        def tab = fx.table("factor")
        def tabrel = fx.table("factorvalrel")
        def tabSC = fx.table("syscod")

        tab.add(id: 1, accessLevel: 1, cod: "_F_1", name: "Тип тяги", fullName: "Тип тяги", ord: "1")
        tab.add(id: 2, accessLevel: 1, cod: "_F_2", name: "Вид грузового вагона", fullName: "Тип вагона", ord: "2")
        tab.add(id: 3, accessLevel: 1, cod: "_F_3", name: "Род груза", fullName: "Род груза", ord: "3")
        tab.add(id: 4, accessLevel: 1, cod: "_F_4", name: "Вид услуги", fullName: "Вид услуги", ord: "4")

        tab.add(id: 5, parent: "1", accessLevel: 1, cod: "_FV_5", name: "Электротяга", fullName: "Электротяга", ord: "5")
        tab.add(id: 6, parent: "1", accessLevel: 1, cod: "_FV_6", name: "Теплотяга", fullName: "Теплотяга", ord: "6")

        tab.add(id: 7, parent: "2", accessLevel: 1, cod: "_FV_7", name: "Платформа", fullName: "Платформа", ord: "7")
        tab.add(id: 8, parent: "2", accessLevel: 1, cod: "_FV_8", name: "Крытый вагон", fullName: "Крытый вагон", ord: "8")
        tab.add(id: 9, parent: "2", accessLevel: 1, cod: "_FV_9", name: "Полувагон", fullName: "Полувагон", ord: "9")
        tab.add(id: 10, parent: "2", accessLevel: 1, cod: "_FV_10", name: "Цистерна", fullName: "Цистерна", ord: "10")

        tab.add(id: 11, parent: "3", accessLevel: 1, cod: "_FV_11", name: "Лес", fullName: "Лес", ord: "11")
        tab.add(id: 12, parent: "3", accessLevel: 1, cod: "_FV_12", name: "Железо", fullName: "Железо", ord: "12")
        tab.add(id: 13, parent: "3", accessLevel: 1, cod: "_FV_13", name: "Уголь", fullName: "Уголь", ord: "13")
        tab.add(id: 14, parent: "3", accessLevel: 1, cod: "_FV_14", name: "Пшеница", fullName: "Пшеница", ord: "14")
        tab.add(id: 15, parent: "3", accessLevel: 1, cod: "_FV_15", name: "Нефть", fullName: "Нефть", ord: "15")

        tab.add(id: 16, parent: "4", accessLevel: 1, cod: "_FV_16", name: "Перевозка грузов", fullName: "Перевозка грузов", ord: "16")
        tab.add(id: 17, parent: "4", accessLevel: 1, cod: "_FV_17", name: "Перевозка порожних вагонов", fullName: "Перевозка порожних вагонов", ord: "17")
        tab.add(id: 18, parent: "4", accessLevel: 1, cod: "_FV_18", name: "Дополнительные сборы", fullName: "Дополнительные сборы", ord: "18")
        tab.add(id: 19, parent: "4", accessLevel: 1, cod: "_FV_19", name: "Предоставление локомотивной тяги для поездной работы", fullName: "Предоставление локомотивной тяги для поездной работы", ord: "19")
        tab.add(id: 20, parent: "4", accessLevel: 1, cod: "_FV_20", name: "Предоставление локомотивной тяги для поездной работы", fullName: "Предоставление локомотивной тяги для поездной работы", ord: "20")
        tab.add(id: 21, parent: "4", accessLevel: 1, cod: "_FV_21", name: "Транспортно-экспедиторские услуги", fullName: "Транспортно-экспедиторские услуги", ord: "21")
        tab.add(id: 22, parent: "4", accessLevel: 1, cod: "_FV_22", name: "Оперирование грузовыми вагонами", fullName: "Оперирование грузовыми вагонами", ord: "22")
        tab.add(id: 23, parent: "4", accessLevel: 1, cod: "_FV_23", name: "Оперирование грузовыми вагонами", fullName: "Оперирование грузовыми вагонами", ord: "23")
        tab.add(id: 24, parent: "4", accessLevel: 1, cod: "_FV_24", name: "Предоставление локомотивной тяги для маневровой работы", fullName: "Предоставление локомотивной тяги для маневровой работы", ord: "24")
        tab.add(id: 25, parent: "4", accessLevel: 1, cod: "_FV_25", name: "Предоставление локомотивной тяги для маневровой работы", fullName: "Предоставление локомотивной тяги для маневровой работы", ord: "25")
        tab.add(id: 26, parent: "4", accessLevel: 1, cod: "_FV_26", name: "Предоставление локомотивной тяги для хозяйственной работы", fullName: "Предоставление локомотивной тяги для хозяйственной работы", ord: "26")


        //CodSys
        long id = 8
        for (long i = 1; i < 27; i++) {
            if (i > 3) {
                tabSC.add(id: id++, cod: "_FV_${i}", linkType: 4L, linkId: i)
            } else {
                tabSC.add(id: id++, cod: "_F_${i}", linkType: 3L, linkId: i)
            }
        }
        //
        long idRel = 1
        tabrel.add(id: idRel++, factor1: 7, factor2: 14)
        tabrel.add(id: idRel++, factor1: 7, factor2: 15)

        tabrel.add(id: idRel++, factor1: 8, factor2: 11)
        tabrel.add(id: idRel++, factor1: 8, factor2: 12)
        tabrel.add(id: idRel++, factor1: 8, factor2: 13)
        tabrel.add(id: idRel++, factor1: 8, factor2: 15)

        tabrel.add(id: idRel++, factor1: 9, factor2: 14)
        tabrel.add(id: idRel++, factor1: 9, factor2: 15)

        tabrel.add(id: idRel++, factor1: 10, factor2: 11)
        tabrel.add(id: idRel++, factor1: 10, factor2: 12)
        tabrel.add(id: idRel++, factor1: 10, factor2: 13)
        tabrel.add(id: idRel++, factor1: 10, factor2: 14)

    }

}
