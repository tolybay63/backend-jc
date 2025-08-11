package tofi.zookeeper;

import jandcode.core.test.*;
import org.apache.curator.framework.*;

public class ZookeeperService_Test extends App_Test {

    ZookeeperService svc;
    CuratorFramework client;

    public void setUp() throws Exception {
        super.setUp();
        //
        svc = app.bean(ZookeeperService.class);
        client = svc.getClient();
    }

    public void test1() throws Exception {
        System.out.println(client);
        System.out.println(client.getState());
    }

}
