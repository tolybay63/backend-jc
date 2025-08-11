package tofi.config;

import java.util.*;

/**
 * Обработчик конфигурации tofi.
 */
public interface TofiConfigHandler {

    /**
     * Сконфигирировать приложение по конфигурации tofi
     *
     * @param tcSvc           сервис конфигурирования
     * @param first           true - это первый вызов метода при старте приложения,
     *                        false - это повторный вызов, когда конфигурация изменилась
     * @param changedSections имена секций конфигурации, которые изменились. При первом вызове
     *                        (first=true) тут будут все доступные имена секций. Если вызов не первый,
     *                        то только измененные секции.
     */
    void tofiConfigure(TofiConfigService tcSvc, boolean first, Set<String> changedSections);

}
