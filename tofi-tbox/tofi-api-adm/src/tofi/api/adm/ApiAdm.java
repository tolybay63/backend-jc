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

    void changePasswd(long user, String oldPasswd, String newPasswd);

    String forgetPasswd(String login, String newPasswd);

    void deleteAuthUser(long id);

    Store loadSql(String sql, String domain);

    void updateEmailAndPhone(Map<String, Object> rec);

}
