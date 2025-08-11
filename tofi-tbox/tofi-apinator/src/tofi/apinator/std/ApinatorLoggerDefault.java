package tofi.apinator.std;

import org.slf4j.*;
import tofi.apinator.*;

public class ApinatorLoggerDefault implements ApinatorLogger {

    protected static Logger log = LoggerFactory.getLogger(ApinatorApi.class);

    public String toString(ApinatorContext ctx) {
        return ctx.getInst().getClass().getSimpleName() + "." + ctx.getMethod().getName() + " => " + ctx.getMethod().toString();
    }

    public void logStart(ApinatorContext ctx) {
        if (!log.isErrorEnabled()) {
            return;
        }
        log.info("API start: " + toString(ctx));
    }

    public void logStop(ApinatorContext ctx) {
        if (!log.isErrorEnabled()) {
            return;
        }
        //
        long tmms = System.currentTimeMillis() - ctx.getStartTime();
        String tm = String.format("%.3f sec", tmms / 1000.0);

        log.info("API stop [" + tm + "]: " + toString(ctx));
    }

}
