package tofi.zookeeper;

import jandcode.commons.datetime.*;
import jandcode.core.test.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.watch.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.junit.jupiter.api.*;

import java.util.*;

public class ClientLearn_Test extends App_Test {

    ZookeeperService svc;
    CuratorFramework client;

    public void setUp() throws Exception {
        super.setUp();
        //
        svc = app.bean(ZookeeperService.class);
        client = svc.getClient();
    }

    //////

    @Test
    public void ls1() throws Exception {
        List<String> ch = client.getChildren().forPath("/");
        System.out.println(ch);
    }

    @Test
    public void data1() throws Exception {
        byte[] dt = client.getData().forPath("/test1/data1");
        System.out.println(dt.length);
        System.out.println(dt);
        System.out.println(new String(dt));
    }

    @Test
    public void data3() throws Exception {
        String a = client.create().orSetData().creatingParentsIfNeeded().forPath("/test1/data2/data3", "qaz1".getBytes());
        System.out.println(a);
    }

    @Test
    public void checkExist1() throws Exception {
        Stat z = client.checkExists().forPath("/qq/www/eee");
        System.out.println(z); //null

        z = client.checkExists().forPath("/test1/data2/data3");
        System.out.println(z);
        System.out.println(z.getDataLength());
    }

    @Test
    public void watch1() throws Exception {
        PersistentWatcher w = new PersistentWatcher(client, "/test1", true);
        w.getListenable().addListener(new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("EVENT: " + event.getPath());
                System.out.println(event);
            }
        });
        w.start();

        Stat z = client.checkExists().forPath("/test1/aaa1");
        if (z == null) {
            client.create().forPath("/test1/aaa1", "q1".getBytes());
        }
        client.setData().forPath("/test1/aaa1", XDateTime.now().toString().getBytes());
        client.setData().forPath("/test1/aaa1", XDateTime.now().toString().getBytes());
        client.setData().forPath("/test1/aaa1", "q1".getBytes());
        client.setData().forPath("/test1/aaa1", "q2".getBytes());

//        while (true) {
        Thread.sleep(3000);
//        }
    }


}
