package tofi.adm.auth;

import jandcode.commons.UtJson;
import jandcode.commons.conf.Conf;
import jandcode.commons.error.XError;
import jandcode.core.BaseComp;
import jandcode.core.auth.*;
import jandcode.commons.variant.VariantMap;
import jandcode.core.dbm.ModelService;
import jandcode.core.dbm.mdb.Mdb;
import tofi.adm.model.dao.auth.AuthDao;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Процессор авторизации через Keycloak
 */
public class KeycloakAuthProcessor extends BaseComp implements AuthProcessor {

    @Override
    public boolean isSupportedAuthToken(AuthToken authToken) {
        return authToken instanceof UserPasswdAuthToken;
    }

    @Override
    public AuthUser login(AuthToken authToken) throws Exception {
        UserPasswdAuthToken token = (UserPasswdAuthToken) authToken;

        // Читаем конфигурацию Keycloak
        Conf keycloakConf = getApp().getConf().getConf("keycloak");
        if (keycloakConf == null) {
            throw new XError("Конфигурация Keycloak не найдена в cfg.cfx");
        }
        String keycloakUrl = keycloakConf.getString("url");
        String realm = keycloakConf.getString("realm");
        String clientId = keycloakConf.getString("clientId");
        String clientSecret = keycloakConf.getString("clientSecret", "");
        
        if (keycloakUrl == null || realm == null || clientId == null) {
            throw new XError("Не все обязательные параметры Keycloak указаны в конфигурации (url, realm, clientId)");
        }

        // Получаем токен от Keycloak
        String accessToken = getKeycloakToken(keycloakUrl, realm, clientId, clientSecret, token.getUsername(), token.getPasswd());
        if (accessToken == null) {
            throw new XErrorAuth(XErrorAuth.msg_invalid_user_passwd);
        }

        // Получаем информацию о пользователе из Keycloak
        Map<String, Object> userInfo = getUserInfoFromKeycloak(keycloakUrl, realm, accessToken);

        if (userInfo == null || userInfo.isEmpty()) {
            throw new XErrorAuth(XErrorAuth.msg_invalid_user_passwd);
        }
        // Преобразуем информацию из Keycloak в формат AuthUser
        VariantMap attrs = new VariantMap();
        attrs.put("id", userInfo.get("sub"));
        attrs.put("username", userInfo.get("preferred_username"));
        attrs.put("email", userInfo.get("email"));
        //attrs.put("name", userInfo.get("name"));
        attrs.put("fullname", userInfo.get("preferred_username"));
        
        // Добавляем дополнительные атрибуты из Keycloak
        if (userInfo.containsKey("given_name")) {
            attrs.put("firstName", userInfo.get("given_name"));
        }
        if (userInfo.containsKey("family_name")) {
            attrs.put("lastName", userInfo.get("family_name"));
        }
        ModelService modelSvc = getApp().bean(ModelService.class);
        Mdb mdb =  modelSvc.getModel().createMdb();
        AuthDao dao = mdb.createDao(AuthDao.class);
        Map<String, Object> attrsjc = dao.getUserInfo(token.getUsername(), token.getPasswd());
        attrs.putAll(attrsjc);

        AuthUser usr = new DefaultAuthUser(attrs);
        AuthService authSvc = getApp().bean(AuthService.class);
        authSvc.setCurrentUser(usr);
        return usr;
    }

    /**
     * Получает access token от Keycloak по username/password
     */
    private String getKeycloakToken(String keycloakUrl, String realm, String clientId, String clientSecret, String username, String password) throws Exception {
        String tokenUrl = keycloakUrl + "/realms/" + realm + "/protocol/openid-connect/token";
        
        // Формируем тело запроса
        String requestBody = "grant_type=password"
                + "&client_id=" + java.net.URLEncoder.encode(clientId, StandardCharsets.UTF_8)
                + "&username=" + java.net.URLEncoder.encode(username, StandardCharsets.UTF_8)
                + "&password=" + java.net.URLEncoder.encode(password, StandardCharsets.UTF_8)
                + "&scope=openid profile email";
        
        if (clientSecret != null && !clientSecret.isEmpty()) {
            requestBody += "&client_secret=" + java.net.URLEncoder.encode(clientSecret, StandardCharsets.UTF_8);
        }

        HttpURLConnection conn = createConnection(tokenUrl, "POST");
        conn.setDoOutput(true);
        
        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = requestBody.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        int responseCode = conn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            // Читаем ошибку от Keycloak для отладки
            String errorResponse = "";
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                StringBuilder error = new StringBuilder();
                String errorLine;
                while ((errorLine = br.readLine()) != null) {
                    error.append(errorLine);
                }
                errorResponse = error.toString();
            } catch (Exception e) {
                // Игнорируем ошибки чтения error stream
            }
            System.err.println("Keycloak token request failed. Response code: " + responseCode);
            System.err.println("URL: " + tokenUrl);
            System.err.println("Request body: " + requestBody.replaceAll("password=[^&]*", "password=***"));
            System.err.println("Error response: " + errorResponse);
            return null;
        }

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            
            Map<String, Object> jsonResponse = UtJson.fromJson(response.toString(), Map.class);
            return (String) jsonResponse.get("access_token");
        }
    }

    /**
     * Получает информацию о пользователе из Keycloak по access token
     */
    private Map<String, Object> getUserInfoFromKeycloak(String keycloakUrl, String realm, String accessToken) throws Exception {
        String userInfoUrl = keycloakUrl + "/realms/" + realm + "/protocol/openid-connect/userinfo";
        
        HttpURLConnection conn = createConnection(userInfoUrl, "GET");
        conn.setRequestProperty("Authorization", "Bearer " + accessToken);

        int responseCode = conn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            return null;
        }

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            
            return UtJson.fromJson(response.toString(), Map.class);
        }
    }

    /**
     * Создает HTTP соединение
     */
    private HttpURLConnection createConnection(String urlString, String method) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Accept", "application/json");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);
        return conn;
    }
}

