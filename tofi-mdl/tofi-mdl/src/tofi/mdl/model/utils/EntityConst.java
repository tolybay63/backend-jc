package tofi.mdl.model.utils;

import jandcode.commons.UtLang;
import jandcode.commons.UtString;
import jandcode.commons.collect.LinkedHashMapNoCase;
import jandcode.commons.error.XError;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EntityConst {

    /**
     * Единицы измерения
     */
    public static long Measure = 1;

    /**
     * Атрибут
     */
    public static long Attrib = 2;

    /**
     * Факторы
     */
    public static long Factor = 3;

    /**
     * Значения факторов
     */
    public static long FactorVal = 4;

    /**
     * Измерители
     */
    public static long Meter = 5;

    /**
     * Показатели
     */
    public static long MeterRate = 6;

    /**
     * Шкалы
     */
    public static long Scale = 7;

    /**
     * Настройки шкалы
     */
    public static long ScaleAsgn = 8;

    /**
     * Роли типы объектов
     */
    public static long Role = 9;

    /**
     * Типы объектов
     */
    public static long Typ = 10;

    /**
     * Классы объектов
     */
    public static long Cls = 11;


    /**
     * Системы кодирования
     */
    public static long SysCoding = 12;

    /*
     * Группа свойств
     */
    public static long PropGr = 13;

    /*
     * Свойства
     */
    public static long Prop = 14;

    /*
     * Группа измерений многомерного свойства
     */
    public static long DimMultiPropGr = 15;

    /*
     * Измерения многомерного свойства
     */
    public static long DimMultiProp = 16;

    /*
     * Элементы измерения многомерного свойства
     */
    public static long DimMultiPropItem = 17;

    /*
     * Группа многомерных свойств
     */
    public static long MultiPropGr = 18;

    /*
     * Многомерные свойства
     */
    public static long MultiProp = 19;

    /*
     * Группа характеристических свойств типа объектов
     */
    public static long TypCharGr = 20;

    /**
     * Отношения между типами объектов
     */
    public static long RelTyp = 21;

    /*
     * отношения между классами
     */
    public static long RelCls = 22;

    /*
     * Группа характеристических свойств отношения между типами объектов
     */
    public static long RelTypCharGr = 23;

    /*
     * Измерение периодов
     */
    public static long DimPeriod = 24;

    /*
     * Группа измерений свойств
     */
    public static long DimPropGr = 25;

    /*
     * Измерения свойств
     */
    public static long DimProp = 26;
    /*
     * Группа измерений объектов и отношений
     */
    public static long DimObjGr = 27;

    /*
     * Измерения объектов и отношений
     */
    public static long DimObj = 28;

    /*
     * Перечень базы данных
     */
    public static long DataBase = 29;

    /*
     * Плоские таблицы
     */
    public static long FlatTable = 30;

    /*
     * Группа стандартных кубов
     */
    public static long CubeSGr = 31;

    /*
     * Стандартные кубы
     */
    public static long CubeS = 32;

    /*
     * Группа источников данных
     */
    public static long SourceStockGr = 33;

    /*
     * Источники
     */
    public static long SourceStock = 34;


    //...

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
        private boolean hasTranslate;

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

        public boolean getHasTranslate() {
            return hasTranslate;
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
                            boolean hasVer, int vis, String icon, boolean hasTranslate) {
        EntityInfo z = new EntityInfo();
        z.numConst = numConst;
        z.codTemplate = genCodPref + codTemplate;
        z.sign = sign;
        z.tableName = tableName;
        z.text = text;
        z.hasVer = hasVer;
        z.hasTranslate = hasTranslate;
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
        // hasVer: false - Неверсионный, true - Версионный
        // hasTranslate: true - Многоязычный, false - нет
        add(Measure, "M_#", "Measure", "Measure", "Единица измерения", false, 1, "measure", true);
        add(Attrib, "A_#", "Attrib", "Attrib", "Атрибут", false, 1, "attrib", true);
        add(Factor, "F_#", "Factor", "Factor", "Фактор", false, 1, "factor", true);
        add(FactorVal, "FV_#", "FactorVal", "Factor", "Значение фактора", false, 1, "factorval", true);
        add(Meter, "I_#", "Meter", "Meter", "Измеритель", false, 1, "meter", true);
        add(MeterRate, "IP_#", "MeterRate", "MeterRate", "Показатель", false, 1, "meterrate", true);
        add(Role, "R_#", "Role", "Role", "Роль типа объектов и отношений", false, 1, "role", true);
        add(Typ, "T_#", "Typ", "Typ", "Тип объектов", true, 1, "typ", true);
        add(Cls, "C_#", "Cls", "Cls", "Класс объектов", true, 1, "cls", true);
        //add(Obj, "O_#", "Obj", "Obj", "Объект", true, 1, "obj");
        add(TypCharGr, "TG_#", "TypCharGr", "TypCharGr", "Группа характеристических свойств типа объектов", false, 1, "typchargr", true);
        add(RelTyp, "RT_#", "RelTyp", "RelTyp", "Отношение между типами объектов", true, 1, "reltyp", true);
        add(SysCoding, "SC_#", "SysCoding", "SysCoding", "Система кодирования", false, 1, "syscoding", true);
        //add(RelObj, "RO_#", "RelObj", "RelObj", "Отношение между объектов", true, 1, "relobj");
        add(RelTypCharGr, "RG_#", "RelTypCharGr", "RelTypCharGr", "Группа характеристических свойств отношения между типами объектов", false, 1, "reltypchargr", true);
        add(PropGr, "PG_#", "PropGr", "PropGr", "Группа свойств", false, 1, "propgr", true);
        add(Prop, "P_#", "Prop", "Prop", "Свойство", false, 0, "prop", true);
        add(DimMultiPropGr, "DMPG_#", "DimMultiPropGr", "DimMultiPropGr", "Группа измерений многомерного свойства", false, 1, "dimmultiprop", true);
        add(DimMultiProp, "DMP_#", "DimMultiProp", "DimMultiProp", "Измерение многомерного свойства", false, 1, "dimmultiprop", true);
        add(Scale, "S_#", "Scale", "Scale", "Шкала", false, 1, "scale", true);
        add(ScaleAsgn, "SA_#", "ScaleAsgn", "scaleasgn", "Настройка шкалы", false, 1, "scaleasgn", true);
        add(MultiPropGr, "MPG_#", "MultiPropGr", "MultiPropGr", "Группа многомерных свойств", false, 1, "multipropgr", true);
        add(MultiProp, "MP_#", "MultiProp", "MultiProp", "Многомерные свойства", false, 1, "multiprop", true);
        add(DimPeriod, "D_#", "DimPeriod", "DimPeriod", "Измерение периодов", false, 1, "dimperiod", true);
        add(DimPropGr, "DPG_#", "DimPropGr", "DimPropGr", "Группа измерения свойств", false, 1, "dimpropgr", true);
        add(DimProp, "DP_#", "DimProp", "DimProp", "Измерение свойств", false, 1, "dimprop", true);
        add(DimObjGr, "DOG_#", "DimObjGr", "DimObjGr", "Группа измерений объектов и отношений", false, 1, "dimobjgr", true);
        add(DimObj, "DO_#", "DimObj", "DimObj", "Измерение объектов и отношений", false, 1, "dimobj", true);
        add(DataBase, "DB_#", "DataBase", "DataBase", "Перечень базы данных", false, 1, "database", true);
        add(RelCls, "RC_#", "RelCls", "RelCls", "Класс отношений", true, 1, "relcls", true);
        add(FlatTable, "FT_#", "FlatTable", "FlatTable", "Плоские таблицы", false, 1, "flattable", true);
        add(DimMultiPropItem, "DMPI_#", "DimMultiPropItem", "DimMultiPropItem", "Элементы измерения многомерного свойства", false, 1, "dimmultipropitem", true);
        add(CubeSGr, "CSG_#", "CubeSGr", "CubeSGr", "Группа стандартных кубов", false, 1, "cubesgr", true);
        add(CubeS, "CS_#", "CubeS", "CubeS", "Стандартные кубы", false, 0, "cubes", true);
        add(SourceStockGr, "DSG_#", "SourceStockGr", "SourceStockGr", "Группа источников данных", false, 0, "sourcestockgr", true);
        add(SourceStock, "DS_#", "SourceStock", "SourceStock", "Источники данных", false, 0, "sourcestock", true);


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
        pat = pat.replace("#", "\\d+");
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
     * @param id  для какого id
     * @return сгенерированный код
     */
    public static String generateCod(long ent, long id) {
        return getCodTemplate(ent).replace("#", "" + id);
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
