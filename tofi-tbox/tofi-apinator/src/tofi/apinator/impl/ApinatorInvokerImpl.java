package tofi.apinator.impl;

import jandcode.commons.*;
import jandcode.commons.conf.*;
import jandcode.commons.error.*;
import jandcode.core.*;
import org.slf4j.*;
import tofi.apinator.*;

import java.lang.reflect.*;
import java.util.*;

public class ApinatorInvokerImpl extends BaseComp implements ApinatorInvoker {

    protected static Logger log = LoggerFactory.getLogger(ApinatorInvoker.class);

    private final List<ApinatorFilter> filters = new ArrayList<>();
    private Conf conf;

    protected void onConfigure(BeanConfig cfg) throws Exception {
        super.onConfigure(cfg);

        this.conf = cfg.getConf();

        // регистрируем все фильтры в отсортированном виде
        List<Conf> filtersConf = UtConf.sortByWeight(cfg.getConf().getConfs("filter"));
        for (Conf fc : filtersConf) {
            ApinatorFilter filter = (ApinatorFilter) getApp().create(fc);
            this.filters.add(filter);
        }

    }

    public Conf getConf() {
        return conf;
    }

    public Object invokeApi(Class cls, Method method, Conf contextConf, Object... args) throws Exception {

        ApinatorLogger logger = getApp().bean(ApinatorService.class).getLogger();

        // создаем контекст
        ApinatorContextImpl context = new ApinatorContextImpl(this, method, getApp().getBeanFactory(), contextConf);

        // регистрируем validateErrors
        ValidateErrors validateErrors = ValidateErrors.create();
        context.getBeanFactory().registerBean(ValidateErrors.class.getName(), validateErrors);

        // создаем экземпляр
        Object inst = context.create(cls);
        context.setInst(inst);

        logger.logStart(context);

        // формируем фильтры
        List<ApinatorFilter> filters = new ArrayList<>(this.filters);
        if (inst instanceof ApinatorFilter) {
            filters.add((ApinatorFilter) inst);
        }


        try {
            // сначала все фильтры before
            for (int i = 0; i < filters.size(); i++) {
                filters.get(i).execApinatorFilter(ApinatorFilterType.before, context);
                validateErrors.checkErrors();
            }

            // затем оригинальный метод
            Object res = method.invoke(inst, args);
            validateErrors.checkErrors();
            context.setResult(res);

            // затем все фильтры after, в обратном порядке
            for (int i = filters.size() - 1; i >= 0; i--) {
                filters.get(i).execApinatorFilter(ApinatorFilterType.after, context);
                validateErrors.checkErrors();
            }

            logger.logStop(context);

        } catch (Throwable e) {

            // запоминаем ошибку
            context.setException(e);

            // выкидываем ее
            throw e;

        } finally {
            // ошибки внутри фильтров игнорируем и логгируем
            for (int i = 0; i < filters.size(); i++) {
                try {
                    filters.get(i).execApinatorFilter(ApinatorFilterType.cleanup, context);
                } catch (Exception ex) {
                    if (log.isErrorEnabled()) {
                        log.error("Error in cleanup filter " + filters.get(i).getClass().getName(), ex);
                    }
                }
            }
        }

        return context.getResult();
    }

}
