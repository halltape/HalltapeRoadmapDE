# Основные команды в Linux

В работе тебе придется часто работать с терминалом (командной строкой).
Команды ниже являются самыми популярными и часто используемыми, поэтому не поленись и потренируйся на них у себя локально.


- ls - список файлов и директорий в текущем каталоге
    ```bash
    ls -l  # подробный список
    ls -a  # включая скрытые файлы
    ```
- cd - смена текущего каталога
    ```bash
    cd /path/to/directory
    cd ..  # выйти из текущей папки на уровень выше
    ```
- pwd - показать текущий путь в каталог, где вы находитесь
    ```bash
    pwd
    ```
- cp - копирование файлов и директорий
    ```bash
    cp source destination
    ```
- mv - перемещение или переименование файлов и директорий
    ```bash
    mv source destination
- touch - создание файлов
    ```bash
    touch file.txt # создать файл
    ```
- vim - встроенный редактор кода
    ```bash
    vim my_code.py
    # Чтобы выйти из vim, нужно нажать esc, потом :wq (это сохраняет файл и выходит из него)
    ```
- rm - удаление файлов и директорий
    ```bash
    rm file
    rm -r directory  # удаление директории и её содержимого
    ```
- cat - вывод содержимого файла.
    ```bash
    cat file
    ```
- grep - поиск строк в файле.
    ```bash
    grep "apple" file
    ```
- chmod - изменение прав доступа к файлам и директориям.
    ```bash
    chmod 755 test.txt
    ```
- echo - запись в файл
  ```bash
  echo "data goes to file" >> data.txt
  ```
- mkdir - создать директорию (папку)
  ```bash
  mkdir halltape_directory
  mkdir -m 755 halltape_directory # Создание каталога с заданными правами доступа
  ```
- history - показать историю команд
  ```bash
  history
  ```

Пример использования chmod (пригодится, когда нужно, чтобы файл мог запускаться сторонней программой и не было конфликтов)
- Создадим файл и изменим права доступа (смотрите, как меняются права доступа в буквенном выражении)
    ```bash
    halltape@MacBookPro Desktop % touch test.txt
    halltape@MacBookPro Desktop % ls -l
    -rw-r--r--   1 halltape  staff    0 Oct  3 12:37 test.txt
    halltape@MacBookPro Desktop % chmod 755 test.txt 
    halltape@MacBookPro Desktop % ls -l
    -rwxr-xr-x   1 halltape  staff    0 Oct  3 12:37 test.txt
    halltape@MacBookPro Desktop % chmod 777 test.txt 
    halltape@MacBookPro Desktop % ls -l
    -rwxrwxrwx   1 halltape  staff    0 Oct  3 12:37 test.txt
    ```

Таблица с обозначениями для chmod
| Число | Право доступа                   |
|-------|---------------------------------|
| 0     | отсутствие прав (---)          |
| 1     | разрешено только исполнение (--x) |
| 2     | разрешена только запись (-w-)   |
| 3     | разрешены запись и исполнение (-wx) |
| 4     | разрешено только чтение (r--)   |
| 5     | разрешены чтение и исполнение (r-x) |
| 6     | разрешены чтение и запись (rw-)  |
| 7     | полные права (rwx)              |
