package tofi.config

import jandcode.core.test.*
import org.junit.jupiter.api.*
import tofi.config.impl.*

class TofiConfigZookeeperUtils_Test extends App_Test {

    @Test
    public void save_load_section1() throws Exception {
        TofiConfigZookeeperUtils u = new TofiConfigZookeeperUtils(app);
        //
        u.removeTofiConfigSection("qaz1")
        u.saveTofiConfigSection("qaz1", [a1: 1, b1: "qqq"])
        u.saveTofiConfigSection("qaz1", [b1: "qqq1", x1: "xxx"])
        def z = u.loadTofiConfigSection("qaz1")
        utils.outMap(z)
    }

    @Test
    public void save_load_section2() throws Exception {
        TofiConfigZookeeperUtils u = new TofiConfigZookeeperUtils(app);
        //
        u.removeTofiConfigSection("qaz2")
        u.saveTofiConfigSection("qaz2", [aa1: 1, ab1: "qqq"])
        u.saveTofiConfigSection("qaz2", [ab1: "qqq1", ax1: "xxx"])
        def z = u.loadTofiConfigSection("qaz2")
        utils.outMap(z)
    }

    @Test
    public void loadAll1() throws Exception {
        TofiConfigZookeeperUtils u = new TofiConfigZookeeperUtils(app);
        //u.removeTofiConfigSection("qaz2")
        def x = u.loadTofiConfig()
        utils.outMap(x)
    }

}
