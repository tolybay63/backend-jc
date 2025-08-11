package tofi.zookeeper.impl;

import jandcode.commons.conf.*;
import jandcode.core.*;
import jandcode.core.std.*;
import org.apache.curator.*;
import org.apache.curator.framework.*;
import org.apache.curator.retry.*;
import org.slf4j.*;
import tofi.zookeeper.*;

public class ZookeeperServiceImpl extends BaseComp implements ZookeeperService, IAppShutdown {

    protected static Logger log = LoggerFactory.getLogger(ZookeeperService.class);

    private CuratorFramework client;

    protected void onConfigure(BeanConfig cfg) throws Exception {
        super.onConfigure(cfg);
        //
    }

    public CuratorFramework getClient() {
        if (this.client == null) {
            synchronized (this) {
                if (this.client == null) {
                    CuratorFramework tmp = createClient();
                    log.info("ZooKeeper client start");
                    tmp.start();
                    log.info("ZooKeeper client started");
                    this.client = tmp;
                }
            }
        }
        return this.client;
    }

    protected CuratorFramework createClient() {
        log.info("ZooKeeper client create");
        CfgService cfgSvc = getApp().bean(CfgService.class);
        Conf x = cfgSvc.getConf().getConf("zookeeper");
        String cn = x.getString("connectionString");
        int sleep = x.getInt("retryPolicy/sleep");
        int retries = x.getInt("retryPolicy/retries");
        //
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(sleep, retries);
        CuratorFramework res = CuratorFrameworkFactory.newClient(cn, retryPolicy);
        //
        return res;
    }

    public void appShutdown() throws Exception {
        if (this.client != null) {
            try {
                this.client.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            this.client = null;
        }
    }
}
