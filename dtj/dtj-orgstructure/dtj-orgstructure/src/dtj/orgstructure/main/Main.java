package dtj.orgstructure.main;

import jandcode.commons.cli.CliLauncher;
import jandcode.core.apx.cli.DbCheckCliCmd;
import jandcode.core.apx.cli.DbCreateCliCmd;
import jandcode.core.cli.AppCliExtension;
import jandcode.core.web.cli.ServeCliCmd;

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
        z.exec();
    }

}
