package dtj.report.dao;

import jandcode.core.dbm.mdb.BaseMdbUtils;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ClearService implements Runnable {
    String dir;
    public ClearService(String dir) {
        this.dir = dir;
    }

    Logger log = Logger.getLogger(ClearService.class.getName());


    @Override
    public void run() {
        //log.info("ClearService запущен для очистки: "+this.dir);

        // Создаем сервис с одним потоком для выполнения задачи
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // Определяем задачу (Runnable), которую нужно выполнять
        Runnable cleanupTask = new Runnable() {
            public void run() {
                System.out.println("Запуск задачи очистки в: " + new java.util.Date());
                cleanUpFiles(dir);
            }
        };

        // Планируем выполнение задачи:
        // 0L      -> начальная задержка (запускается немедленно при старте)
        // 1L      -> интервал между выполнениями
        // TimeUnit.HOURS -> единица измерения интервала (часы)
        scheduler.scheduleAtFixedRate(cleanupTask, 0L, 1L, TimeUnit.MINUTES);

        System.out.println("Сервис очистки запущен. Ожидание первого выполнения...");

    }

    private static void cleanUpFiles(String directoryPath) {
        File dir = new File(directoryPath);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Директория не найдена или не является директорией: " + directoryPath);
            return;
        }

        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                // Пример фильтрации: удаляем только файлы с расширением .tmp
                if (file.isFile() && file.getName().toLowerCase().endsWith(".tmp")) {
                    if (file.delete()) {
                        System.out.println("Удален файл: " + file.getName());
                    } else {
                        System.err.println("Не удалось удалить файл: " + file.getName());
                    }
                }
            }
        }
    }



}
