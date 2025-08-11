package tofi.api.dta.model.utils;

import jandcode.commons.*;
import jandcode.commons.collect.*;
import jandcode.commons.error.*;

import java.util.*;
import java.util.regex.*;

public class EntityConst {

    /**
     * Объект
     */
    public static long Obj = 1;

    /**
     * Отношение
     */
    public static long RelObj = 2;

    /**
     * Информация о сущности
     */
    public static class EntityInfo {

        private long numConst;
        private String codTemplate;
        private String sign;
        private String tableName;
        private String text;
        private boolean hasVer;
        private String icon;
        private int vis;

        /**
         * Числовая константа для сущности = код значения фактора с id=50
         */
        public long getNumConst() {
            return numConst;
        }

        /**
         * Шаблон кода
         */
        public String getCodTemplate() {
            return codTemplate;
        }

        /**
         * Имя таблицы
         */
        public String getTableName() {
            return tableName;
        }

        /**
         * Код сущности
         */
        public String getSign() {
            return sign;
        }

        /**
         * Наименование сущности
         */
        public String getText() {
            return text;
        }

        /**
         * Имеет версию?
         */
        public boolean getHasVer() {
            return hasVer;
        }

        public int getVis() {
            return vis;
        }

        public String getIcon() {
            return icon;
        }
    }

    private static final LinkedHashMap<Long, EntityInfo> entityInfos = new LinkedHashMap<Long, EntityInfo>();
    private static final LinkedHashMapNoCase<EntityInfo> entityInfosBySign = new LinkedHashMapNoCase<EntityInfo>();

    public static String genCodPref = "_";

    private static void add(long numConst, String codTemplate, String sign, String tableName, String text,
                            boolean hasVer, int vis, String icon) {
        EntityInfo z = new EntityInfo();
        z.numConst = numConst;
        z.codTemplate = genCodPref + codTemplate;
        z.sign = sign;
        z.tableName = tableName;
        z.text = text;
        z.hasVer = hasVer;
        z.icon = icon;
        z.vis = vis;
        //
        if (entityInfos.containsKey(numConst)) {
            throw new XError("TofiEntityConst: duplicate numConst {0}", numConst);
        }
        entityInfos.put(numConst, z);
        //
        if (entityInfosBySign.containsKey(z.getSign())) {
            throw new XError("TofiEntityConst: duplicate tableName {0}", numConst);
        }
        entityInfosBySign.put(z.getSign(), z);
    }

    static {

        // дополнительная информация о сущностях
        // false: Неверсионный  true: Версионный
        add(Obj, "O_#_#", "Obj", "Obj", "Объект", true, 1, "obj");
        add(RelObj, "RO_#_#", "RelObj", "RelObj", "Отношение между объектами", true, 1, "relobj");
    }

    ////// утилиты

    /**
     * Список сущностей
     */
    public static Collection<EntityInfo> getEntityInfos() {
        return entityInfos.values();
    }

    /**
     * Информация о сущности по коду сущности
     */
    public static EntityInfo getEntityInfo(long ent) {
        EntityInfo z = entityInfos.get(ent);
        if (z == null) {
            throw new XError(UtLang.t("Не найдена сущность с кодом {0}", ent));
        }
        return z;
    }

    /**
     * Информация о сущности по коду сущности
     */
    public static EntityInfo getEntityInfo(String sign) {
        EntityInfo z = entityInfosBySign.get(sign);
        if (z == null) {
            throw new XError(UtLang.t("Не найдена сущность с именем {0}", sign));
        }
        return z;
    }

    /**
     * Шаблон кода сущности по коду сущности
     */
    public static String getCodTemplate(long ent) {
        return getEntityInfo(ent).getCodTemplate();
    }

    /**
     * Числовая константа сущности для имени таблицы
     */
    public static long getNumConst(String sign) {
        return getEntityInfo(sign).getNumConst();
    }

    /**
     * Проверка, что код сгенерирован
     *
     * @param ent для кого
     * @param cod код
     * @return true - сгенерирован
     */
    public static boolean isCodGenerated(long ent, String cod) {
        if (UtString.empty(cod)) {
            return false;
        }
        String pat = getCodTemplate(ent);
        pat = pat.replaceAll("#", "\\d+"); // todo
        Pattern p = Pattern.compile(pat, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(cod);
        return m.matches();
    }

    /**
     * Проверка, что код сгенерирован. Проверяются все варианты.
     *
     * @param cod код
     * @return true - сгенерирован
     */
    public static boolean isCodGenerated(String cod) {
        if (UtString.empty(cod)) {
            return false;
        }
        for (EntityInfo z : getEntityInfos()) {
            if (isCodGenerated(z.getNumConst(), cod)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Генерация кода
     *
     * @param ent для кого
     * @param id1  для cls|relcls
     * @param id2  для obj|relobj
     * @return сгенерированный код
     */
    public static String generateCod(long ent, long id1, long id2) {
        String tmp1 = getCodTemplate(ent).replaceFirst("#", "" + id1);
        String  tmp2 = tmp1.replaceFirst("#", "" + id2);
        return tmp2;
    }

    /**
     * Проверка, что код соответсвует шаблону (состоит из анг. алф., цифр и знаков: -, _, /)
     *
     * @param cod код
     * @return true - соответсвует
     */
    public static boolean isCodValid(String cod) {
        if (cod.startsWith(genCodPref)) return false;

        if (!UtString.empty(cod)) {
            String pText = "[a-zA-Z0-9_\\-\\/\\.]*";
            Pattern p = Pattern.compile(pText, Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(cod);

            return m.matches();
        } else {
            return true;
        }
    }


}
