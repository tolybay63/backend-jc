
package tofi.mdl.model.utils;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.translate.Translate;
import com.google.cloud.translate.TranslateOptions;
import com.google.cloud.translate.Translation;
import jandcode.commons.variant.IVariantMap;
import jandcode.core.dbm.mdb.Mdb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class Translator {
    private Translate translate;
    Mdb mdb;
    IVariantMap map;
    private static final Logger logger = LoggerFactory.getLogger(Translator.class);

    // Новый конструктор без параметров (данные берутся из окружения)
    public Translator(Mdb mdb) {
        this.mdb = mdb;
        initializeTranslateClient();
    }

    void initializeTranslateClient() {
        if (translate == null) {
            try {
                map =  mdb.getApp().getEnv().getProperties();

                String jsonConfig = map.getString("GOOGLE_APPLICATION_CREDENTIALS_JSON");

                // Если в .env были экранированные переносы \\n, исправляем их для Google SDK
                if (jsonConfig != null) {
                    // В некоторых случаях парсеры добавляют лишние кавычки по краям
                    if (jsonConfig.startsWith("\"") && jsonConfig.endsWith("\"")) {
                        jsonConfig = jsonConfig.substring(1, jsonConfig.length() - 1);
                    }
                }

                GoogleCredentials credentials = GoogleCredentials.fromStream(
                        new ByteArrayInputStream(Objects.requireNonNull(jsonConfig).getBytes(StandardCharsets.UTF_8))
                );

                translate = TranslateOptions.newBuilder()
                        .setCredentials(credentials)
                        .build()
                        .getService();

                logger.info("Google Translate client initialized via Environment Variables.");
            } catch (Exception e) {
                logger.error("Failed to initialize Google Translate client.", e);
            }
        }
    }

    public String translateText(String text, String fromLanguage, String toLanguage) {
        if (translate == null) return text;

        try {
            Translation translation = translate.translate(
                    text,
                    Translate.TranslateOption.sourceLanguage(fromLanguage),
                    Translate.TranslateOption.targetLanguage(toLanguage)
            );
            return translation.getTranslatedText();
        } catch (Exception e) {
            logger.error("Translation error", e);
            return text;
        }
    }
}


/*
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.translate.Translate;
import com.google.cloud.translate.TranslateOptions;
import com.google.cloud.translate.Translation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;

public class Translator {
    Translate translate;
    String path;

    public Translator(String path) {
        this.path = path;
        initializeTranslateClient();
    }


    void initializeTranslateClient() {
        Logger logger = LoggerFactory.getLogger(Translator.class);

        if (translate == null) {
            try {

                GoogleCredentials credentials = GoogleCredentials.fromStream(
                        new FileInputStream(path)
                );
                translate = TranslateOptions.newBuilder()
                        .setCredentials(credentials)
                        .build()
                        .getService();
                logger.info("Google Translate client initialized.");
            } catch (Exception e) {
                logger.error("Failed to initialize Google Translate client.", e);
            }
        }
    }

    public String translateText(String text, String fromLanguage, String toLanguage) {
        String s = "";
        try {
            Translation translation = translate.translate(
                    text,
                    Translate.TranslateOption.sourceLanguage(fromLanguage),
                    Translate.TranslateOption.targetLanguage (toLanguage)
                    );
            s = translation.getTranslatedText();
        } catch (Exception e) {
            e.printStackTrace();
            s = text;
        }
        return s;
    }

}
*/
