package tofi.config;

import jandcode.commons.*;
import jandcode.commons.conf.*;
import jandcode.core.*;
import jandcode.core.db.*;

import java.util.*;

/**
 * Различные утилиты для упрощения конфигурирования
 */
public class TofiConfigUtils extends BaseComp {

    public TofiConfigUtils(App app) {
        setApp(app);
    }

    /**
     * Сконфигурировать DbSource.
     * Свойство dbdriver игнорируется.
     *
     * @param dbSource что конфигурировать
     * @param conf     конфигурация
     */
    public void configureDbSource(DbSource dbSource, Conf conf) {
        List<String> propNames = UtConf.getPropNames(conf, false);
        for (String p : propNames) {
            if ("dbdriver".equals(p)) {
                continue;
            }
            dbSource.getProps().put(p, conf.getValue(p));
        }
    }

}
