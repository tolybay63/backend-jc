package tofi.api.adm.model.utils;

import jandcode.commons.UtJson;
import jandcode.core.App;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Мини‑клиент для Keycloak Admin API (создание пользователя, пароль)
 */
public class KeycloakAdminClient {

    private final App app;

    public KeycloakAdminClient(App app) {
        this.app = app;
    }

    public String createUser(String username, String email, boolean emailVerified) throws Exception {
        String adminToken = getAdminToken();
        String base = app.getConf().getConf("keycloak").getString("url");
        String realm = app.getConf().getConf("keycloak").getString("realm");
        String url = base + "/admin/realms/" + realm + "/users";
        String body = UtJson.toJson(Map.of(
                "username", username,
                "email", email,
                "enabled", true,
                "emailVerified", emailVerified
        ));
        HttpURLConnection conn = postJson(url, body, adminToken);
        int code = conn.getResponseCode();
        if (code != 201 && code != 204) {
            throw new RuntimeException("Keycloak create user failed: " + code + " " + readError(conn));
        }
        // Получим id по username
        String findUrl = url + "?username=" + java.net.URLEncoder.encode(username, StandardCharsets.UTF_8);
        HttpURLConnection get = getJson(findUrl, adminToken);
        if (get.getResponseCode() != 200) {
            throw new RuntimeException("Keycloak fetch user failed: " + get.getResponseCode());
        }
        String json = readOk(get);
        List list = (List) UtJson.fromJson(json, List.class);
        if (list.isEmpty()) throw new RuntimeException("User not found after create");
        Map first = (Map) list.get(0);
        return String.valueOf(first.get("id"));
    }

    public void setUserPassword(String userId, String password, boolean temporary) throws Exception {
        String adminToken = getAdminToken();
        String base = app.getConf().getConf("keycloak").getString("url");
        String realm = app.getConf().getConf("keycloak").getString("realm");
        String url = base + "/admin/realms/" + realm + "/users/" + userId + "/reset-password";
        String body = UtJson.toJson(Map.of(
                "type", "password",
                "value", password,
                "temporary", temporary
        ));
        HttpURLConnection conn = putJson(url, body, adminToken);
        int code = conn.getResponseCode();
        if (code != 204) {
            throw new RuntimeException("Keycloak set password failed: " + code + " " + readError(conn));
        }
    }

    private String getAdminToken() throws Exception {
        var kc = app.getConf().getConf("keycloak");
        String base = kc.getString("url");
        String tokenUrl = base + "/realms/master/protocol/openid-connect/token";
        String adminUser = kc.getString("adminUsername");
        String adminPass = kc.getString("adminPassword");
        String body = "grant_type=password&client_id=admin-cli"
                + "&username=" + enc(adminUser)
                + "&password=" + enc(adminPass);
        HttpURLConnection conn = formUrlencoded(tokenUrl, body);
        if (conn.getResponseCode() != 200) {
            throw new RuntimeException("Keycloak admin token failed: " + conn.getResponseCode() + " " + readError(conn));
        }
        String json = readOk(conn);
        Map m = (Map) UtJson.fromJson(json, Map.class);
        return String.valueOf(m.get("access_token"));
    }

    private static String enc(String s) {
        return java.net.URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    private static HttpURLConnection formUrlencoded(String url, String body) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return conn;
    }

    private static HttpURLConnection postJson(String url, String body, String bearer) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Authorization", "Bearer " + bearer);
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return conn;
    }

    private static HttpURLConnection putJson(String url, String body, String bearer) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Authorization", "Bearer " + bearer);
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return conn;
    }

    private static HttpURLConnection getJson(String url, String bearer) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("Authorization", "Bearer " + bearer);
        return conn;
    }

    private static String readOk(HttpURLConnection conn) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }
        return sb.toString();
    }

    private static String readError(HttpURLConnection conn) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
