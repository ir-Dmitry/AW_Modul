# Основная настройка

### На клиентских пк в файле C:\Windows\System32\drivers\etc\hosts добавить (пример)

```
192.168.0.209	app.aw.it-albion.ru
```

### Установить

- Установить Python

```
https://www.python.org/
```

- Установить Java JRE

```
https://www.java.com/ru/download/
```

### Устанавливаем библиотеку для создания виртуального окружения

```commandline
pip install virtualenv
```

### В PowerShell от имени администратора:

```commandline
Set-ExecutionPolicy RemoteSigned
    A
```

### Создаем виртуальное окружение

```commandline
python venv venv
```

### Активируем виртуальное окружение

```commandline
venv/Scripts/activate
```

### Установка пакетов (Для винды):

```commandline
pip install analytic-workspace-client
pip install python-dotenv
pip install pandas
pip install "fastapi[standard]"
```

### Или

```
pip install -r requirements.txt
```

### Для UNIX-подобных систем возможно нужно будет дописывать флаг

```
--break-system-packages
```

### Запуск обработчика вручную

```commandline
fastapi dev main.py
```

### Деактивация виртуального окружения

```commandline
deactivate
```

### Устанавливаем расширение в браузер для работы с дашбордом

```
Перейдя по ссылке ниже нажимаем кнопку "Установить"
https://chromewebstore.google.com/detail/cors-helper/jckpaobldaolbbchaeicopkcljcaiaka
После чего в меню расширения в правом верхнем углу выбираем установившееся расширение
Далее в открывшемся окошке включаем Cross-Origin Resource Sharing, заходим в меню "More Details".
Внтури него переходим в "White List" где создаем новый фильтр (New filter patterns).
В поле "Url Patterns" вводим URL, по которому идет подключение к модулю в виде *://ВАШ_URL/*.
Для локального подключения используйте *://127.0.0.1/*
```

НЕ актуально.

### Настройка nginx

### Выпуск сертификатов

# **\*\***\*\***\*\***\*\***\*\***\*\***\*\***\_**\*\***\*\***\*\***\*\***\*\***\*\***\*\***

# PySpark

```
pip install pyspark
pip install setuptools

pip install pyspark[sql]
```
