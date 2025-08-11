package example.apinator1

import example.apinator1.dao.*
import jandcode.core.apx.test.*
import org.junit.jupiter.api.*

class Example1_dao_Test extends Apx_Test {

    @Test
    void getUserNames1() throws Exception {
        def dao = mdb.create(Example1_dao)
        def names = dao.getUserNames()
        println names
    }

    @Test
    void getFactorNames1() throws Exception {
        def dao = mdb.create(Example1_dao)
        def names = dao.getFactorNames()
        println names
    }

    @Test
    void getFactorNamesDirect1() throws Exception {
        def dao = mdb.create(Example1_dao)
        def names = dao.getFactorNamesDirect()
        println names
    }

    @Test
    void getFactorNamesDirectJava1() throws Exception {
        def dao = mdb.create(Example2Java_dao)
        def names = dao.getFactorNamesDirect()
        println names
    }

}
