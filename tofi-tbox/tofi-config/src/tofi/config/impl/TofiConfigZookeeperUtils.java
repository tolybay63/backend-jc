package tofi.config.impl;

import jandcode.commons.*;
import jandcode.commons.conf.*;
import jandcode.commons.error.*;
import jandcode.commons.variant.*;
import jandcode.core.*;
import org.apache.curator.framework.*;
import tofi.config.*;
import tofi.zookeeper.*;

import java.util.*;

/**
 * Утилиты для чтения/записи в zookeeper конфигурации tofi-config.
 */
public class TofiConfigZookeeperUtils extends BaseComp {

    private String basePath = TofiConfigConsts.ZOO_BASE_PATH;

    public TofiConfigZookeeperUtils(App app) {
        setApp(app);
    }

    private String sectionPath(String sectionName) {
        return UtFile.join(basePath, sectionName);
    }

    private CuratorFramework getClient() {
        return getApp().bean(ZookeeperService.class).getClient();
    }

    /**
     * Сохранить конфигурацию.
     * Накладывается на текущую.
     *
     * @param conf конфигурация. Верхний уровень - секция.
     */
    public void saveTofiConfig(Conf conf) {
        if (conf == null) {
            return;
        }
        for (Conf x : conf.getConfs()) {
            saveTofiConfigSection(x.getName(), x);
        }
    }

    /**
     * Сохранить конфигурацию одной секции
     *
     * @param sectionName имя секции
     * @param attrs       атрибуты
     * @return true - если конфигурация была изменена физически
     */
    public boolean saveTofiConfigSection(String sectionName, Map<String, Object> attrs) {
        try {
            IVariantMap prevConf = loadTofiConfigSection(sectionName);
            String crc1 = UtString.md5Str(UtJson.toJson(prevConf));
            prevConf.putAll(attrs);
            String crc2 = UtString.md5Str(UtJson.toJson(prevConf));
            if (crc1.equals(crc2)) {
                return false;
            }
            //
            CuratorFramework client = getClient();
            String path = sectionPath(sectionName);
            Map<String, Object> tmp = new LinkedHashMap<>();
            for (String k : prevConf.keySet()) {
                Object v = prevConf.get(k);
                if (v instanceof Map) {
                    continue;
                }
                if (v instanceof Collection) {
                    continue;
                }
                tmp.put(k, v);
            }
            String json = UtJson.toJson(tmp);
            byte[] data = json.getBytes();
            client.create().orSetData().creatingParentsIfNeeded().forPath(path, data);
            //
            return true;
        } catch (Exception e) {
            throw new XErrorWrap(e);
        }
    }

    /**
     * Загрузить всю конфигурацию
     */
    public Conf loadTofiConfig() {
        Conf res = Conf.create("tofi-config");
        List<String> sections = loadTofiConfigSectionNames();
        for (String sectionName : sections) {
            Conf sect = res.findConf(sectionName, true);
            IVariantMap attrs = loadTofiConfigSection(sectionName);
            sect.putAll(attrs);
        }
        return res;
    }

    /**
     * Загрузить конфигурацию одной секции. Если такой секции нет, все равно возвращается конфигурация,
     * но пустая.
     *
     * @param sectionName имя секции
     */
    public IVariantMap loadTofiConfigSection(String sectionName) {
        try {
            IVariantMap res = new VariantMap();

            CuratorFramework client = getClient();
            String path = sectionPath(sectionName);

            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                try {
                    String jsonData = new String(data);
                    Map<String, Object> json = (Map<String, Object>) UtJson.fromJson(jsonData);
                    if (json != null) {
                        for (String k : json.keySet()) {
                            Object v = json.get(k);
                            if (v instanceof Map) {
                                continue;
                            }
                            if (v instanceof Collection) {
                                continue;
                            }
                            res.put(k, v);
                        }
                    }
                } catch (Exception e) {
                    //ignore?
                }

            }
            return res;
        } catch (Exception e) {
            throw new XErrorWrap(e);
        }

    }

    /**
     * Удалить конфигурацию одной секции
     *
     * @param sectionName имя секции
     */
    public void removeTofiConfigSection(String sectionName) {
        try {
            CuratorFramework client = getClient();
            String path = sectionPath(sectionName);
            if (client.checkExists().forPath(path) != null) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
            }
        } catch (Exception e) {
            throw new XErrorWrap(e);
        }
    }

    /**
     * Получить имена секций
     */
    public List<String> loadTofiConfigSectionNames() {
        try {
            List<String> res = new ArrayList<>();
            CuratorFramework client = getClient();
            String path = this.basePath;
            if (client.checkExists().forPath(path) != null) {
                res = client.getChildren().forPath(path);
            }
            return res;
        } catch (Exception e) {
            throw new XErrorWrap(e);
        }
    }

}
