
package tofi.mdl.model.utils;

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
