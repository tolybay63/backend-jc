package tofi.mdl;

import jandcode.commons.UtConf;
import jandcode.commons.named.NamedList;
import jandcode.core.apx.test.Apx_Test;
import jandcode.core.dbm.domain.Domain;
import jandcode.core.dbm.domain.DomainService;
import jandcode.core.dbm.domain.Field;
import org.junit.jupiter.api.Test;

public class Domain_Test extends Apx_Test {

    /**
     * Список всех доменов и их полей
     */
    @Test
    public void domain_list() throws Exception {
        DomainService domainSvc = getModel().bean(DomainService.class);
        NamedList<Domain> domains = domainSvc.getDomains();
        for (Domain domain : domains) {
            System.out.println(domain.getName());
            for (Field field : domain.getFields()) {
                System.out.println("  " + field.getName() + ": " + field.getDbDataType().getName());
            }
        }
    }

    /**
     * Конфигурация домена
     */
    @Test
    public void domain_conf() throws Exception {
        DomainService domainSvc = getModel().bean(DomainService.class);
        Domain d = domainSvc.getDomain("factor");

        String s = UtConf.save(d.getConf()).toString();
        System.out.println(s);

    }


}
