package tofi.zookeeper;

import jandcode.core.*;
import org.apache.curator.framework.*;

/**
 * Сервис для доступа к zookeeper
 */
public interface ZookeeperService extends Comp {

    /**
     * Глобальный клиент
     */
    CuratorFramework getClient();

}
