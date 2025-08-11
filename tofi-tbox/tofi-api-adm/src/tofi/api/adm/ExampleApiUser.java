package tofi.api.adm;

import jandcode.core.store.*;

/**
 * Пример интерфейса доступа к пользователям
 */
public interface ExampleApiUser {

    /**
     * Получить всех пользователей со всеми полями.
     */
    Store getAllUsersAsStore();

}
