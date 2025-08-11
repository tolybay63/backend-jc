package tofi.apinator;

import jandcode.commons.named.*;
import jandcode.core.*;

public interface ApinatorService extends Comp {

    NamedList<ApinatorApi> getApis();

    ApinatorApi getApi(String name);

    ApinatorLogger getLogger();

}
