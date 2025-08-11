package tofi.config;

import jandcode.commons.conf.*;
import jandcode.core.*;

import java.util.*;

/**
 * Сервис содержит и управляет конфигурацией кластера ТОФИ.
 */
public interface TofiConfigService extends Comp {


    /**
     * Собранная и готовая к использованию конфигурация tofi.
     * Только для чтения! Данные из этого объекта кешировать нельзя, в любой момент
     * может быть создана новая версия конфигурации.
     */
    Conf getConf();

    /**
     * true - zookeeper разрешен и настройки будем брать оттуда. Иначе все настройки берутся только из
     * конфигурации приложения. Этот режим обычно будет использоваться при разработке.
     * <p>
     * app.cfx: tofi/tofi-config/zookeeperEnable
     */
    boolean isZookeeperEnable();

    /**
     * Обновить конфигурацию
     *
     * @param conf конфигурация, которая наложится на текущую
     * @return имена измененнных секций
     */
    List<String> updateConf(Conf conf);

}
