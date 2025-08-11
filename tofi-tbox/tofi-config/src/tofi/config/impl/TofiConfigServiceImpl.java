package tofi.config.impl;

import jandcode.commons.*;
import jandcode.commons.conf.*;
import jandcode.commons.variant.*;
import jandcode.core.*;
import jandcode.core.std.*;
import org.apache.curator.framework.recipes.watch.*;
import org.slf4j.*;
import tofi.config.*;
import tofi.zookeeper.*;

import java.util.*;

public class TofiConfigServiceImpl extends BaseComp implements TofiConfigService, IAppLoaded, IAppShutdown {

    protected static Logger log = LoggerFactory.getLogger(TofiConfigService.class);

    private Conf conf;
    private Conf origConf;
    private Conf zooConf;
    private boolean zookeeperEnable = true;
    private PersistentWatcher zooWatcher;


    protected void onConfigure(BeanConfig cfg) throws Exception {
        super.onConfigure(cfg);
        //
        Conf x = getApp().getConf().getConf("tofi/tofi-config");

        //
        this.zookeeperEnable = x.getBoolean("zookeeperEnable", true);

        // оригинальная конфигурация из cfg/tofi-config
        this.origConf = Conf.create("tofi-config");
        Conf x1 = getApp().bean(CfgService.class).getConf().getConf("tofi/tofi-config");
        this.origConf.join(x1);
    }

    public Conf getConf() {
        checkConf();
        return conf;
    }

    private void checkConf() {
        if (this.conf == null) {
            synchronized (this) {
                if (this.conf == null) {
                    this.conf = createConf();
                }
            }
        }
    }

    protected Conf createConf() {
        Conf x = Conf.create("tofi-config");

        //
        x.join(this.origConf);
        if (isZookeeperEnable()) {
            log.info("ZooKeeper loadTofiConf");
            TofiConfigZookeeperUtils zooUt = new TofiConfigZookeeperUtils(getApp());
            this.zooConf = zooUt.loadTofiConfig();
            x.join(this.zooConf);
        }

        return x;
    }

    public boolean isZookeeperEnable() {
        return zookeeperEnable;
    }

    //////

    public void appLoaded() throws Exception {
        // собираем конфигурацию
        Conf conf = getConf();

        // уведомляем всех причастных
        Set<String> changedSections = new HashSet<>(conf.keySet());
        for (TofiConfigHandler h : getApp().impl(TofiConfigHandler.class)) {
            h.tofiConfigure(this, true, changedSections);
        }

        // wather
        if (isZookeeperEnable()) {
            log.info("ZooKeeper watcher start");
            this.zooWatcher = new PersistentWatcher(
                    getApp().bean(ZookeeperService.class).getClient(),
                    TofiConfigConsts.ZOO_BASE_PATH, true
            );
            this.zooWatcher.getListenable().addListener((event) -> {
                String section = UtFile.filename(event.getPath());

                // загружаем новую версию конфигурации
                TofiConfigZookeeperUtils zooUt = new TofiConfigZookeeperUtils(getApp());
                IVariantMap newAttrs = zooUt.loadTofiConfigSection(section);
                conf.findConf(section, true).putAll(newAttrs);

                // уведомляем всех причастных
                Set<String> changedSections1 = new HashSet<>(conf.keySet());
                changedSections1.add(section);
                for (TofiConfigHandler h : getApp().impl(TofiConfigHandler.class)) {
                    h.tofiConfigure(this, false, changedSections1);
                }
            });
            this.zooWatcher.start();
            log.info("ZooKeeper watcher started");
        }

    }

    public void appShutdown() throws Exception {
        if (this.zooWatcher != null) {
            this.zooWatcher.close();
            this.zooWatcher = null;
        }
    }

    public List<String> updateConf(Conf conf) {
        List<String> res = new ArrayList<>();
        if (conf == null) {
            return res;
        }
        log.info("ZooKeeper saveTofiConfig");
        TofiConfigZookeeperUtils zooUt = new TofiConfigZookeeperUtils(getApp());
        for (Conf sectionConf : conf.getConfs()) {
            boolean saved = zooUt.saveTofiConfigSection(sectionConf.getName(), sectionConf);
            if (saved) {
                res.add(sectionConf.getName());
            }
        }
        return res;
    }
}
