package tofi.api.adm;

import jandcode.core.store.Store;

import java.util.*;

public interface ApiAdm {

   Map<String, Object> getUserInfo(String login, String passwd, String app);

    /**
     *
     * @param id AuthUser
     * @return StoreRecord AuthUser
     */
    Store loadAuthUser(long id);

    long regUser(Map<String, Object> rec);

    void deleteAuthUser(long id);

    Store loadSql(String sql, String domain);
}
