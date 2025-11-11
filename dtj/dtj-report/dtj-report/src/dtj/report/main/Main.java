package dtj.report.main;

import dtj.report.dao.ClearService;
import jandcode.commons.cli.CliLauncher;
import jandcode.core.apx.cli.DbCheckCliCmd;
import jandcode.core.apx.cli.DbCreateCliCmd;
import jandcode.core.cli.AppCliExtension;
import jandcode.core.web.cli.ServeCliCmd;

import java.io.File;

public class Main {

    public static void main(String[] args) {
        new Main().run(args);
    }

    public void run(String[] args) {
        CliLauncher z = new CliLauncher(args);
        z.addExtension(new AppCliExtension());
        z.addCmd("serve", new ServeCliCmd());
        z.addCmd("db-check", new DbCheckCliCmd());
        z.addCmd("db-create", new DbCreateCliCmd());
        clear(z.getAppDir() + File.separator + "reports");
        z.exec();
    }

    public void clear(String dir) {

        ClearService clearService = new ClearService(dir);
        Thread thread = new Thread(clearService);
        thread.start();
    }

}
