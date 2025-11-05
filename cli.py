import toml
import sys
import os
import gzip
import re
from urllib import request as url_request
from urllib.error import URLError, HTTPError


def load_config(config_path):
    """Загрузка конфигурации из TOML файла"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return toml.load(f)
    except FileNotFoundError:
        print(f"Ошибка: Файл конфигурации не найден: {config_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка загрузки конфигурации: {e}")
        sys.exit(1)


def validate_config(config):
    """Валидация параметров конфигурации (расширенная для Этапа 2)"""
    errors = []

    # Проверка имени пакета
    if not config.get('package_name'):
        errors.append("Имя пакета (package_name) не может быть пустым")

    # Проверка режима работы
    valid_modes = ['local', 'remote', 'test']
    mode = config.get('working_mode')
    if mode not in valid_modes:
        errors.append(f"Режим работы (working_mode) должен быть одним из: {', '.join(valid_modes)}")

    # Проверка URL/пути репозитория
    repo_url = config.get('repository_url', '')
    if not repo_url:
        errors.append("URL репозитория (repository_url) не может быть пустым")

    # --- Новые проверки для Этапа 2 ---
    if mode == 'remote':
        # Для удаленного режима нужен http/https и доп. параметры
        if not repo_url.startswith(('http://', 'https://')):
            errors.append("Для 'remote' режима repository_url должен начинаться с http:// или https://")
        if not config.get('distribution'):
            errors.append("distribution (напр. 'jammy') обязателен для 'remote' режима")
        if not config.get('component'):
            errors.append("component (напр. 'main') обязателен для 'remote' режима")
        if not config.get('architecture'):
            errors.append("architecture (напр. 'amd64') обязателен для 'remote' режима")

    elif mode == 'local':
        # Для локального режима, repository_url - это путь к файлу (напр. 'packages.gz')
        if repo_url.startswith(('http://', 'https://')):
            errors.append("Для 'local' режима repository_url должен быть локальным путем, а не URL")
    # --- Конец новых проверок ---

    # Проверка глубины анализа
    depth = config.get('max_depth', 1)
    if not isinstance(depth, int) or depth < 1 or depth > 20:
        errors.append("Глубина анализа (max_depth) должна быть целым числом от 1 до 20")

    # Проверка фильтра
    filter_str = config.get('filter_substring', '')
    if not isinstance(filter_str, str):
        errors.append("Подстрока фильтра (filter_substring) должна быть строкой")

    return errors


def get_packages_data(config):
    """
    Загружает и распаковывает файл Packages.gz
    в зависимости от режима работы.
    """
    mode = config['working_mode']
    repo_url = config['repository_url']

    print(f"Режим работы: {mode}. Получение данных...")

    try:
        if mode == 'remote':
            # 1. Собираем полный URL к файлу Packages.gz
            # Пример: http://archive.ubuntu.com/ubuntu/dists/jammy/main/binary-amd64/Packages.gz
            full_url = (
                f"{repo_url}/dists/{config['distribution']}/"
                f"{config['component']}/binary-{config['architecture']}/Packages.gz"
            )
            print(f"Загрузка из: {full_url}")

            with url_request.urlopen(full_url) as response:
                # 2. Распаковываем Gzip "на лету"
                with gzip.GzipFile(fileobj=response) as gzip_file:
                    # 3. Читаем и декодируем
                    return gzip_file.read().decode('utf-8')

        elif mode == 'local' or mode == 'test':
            # В локальном режиме repository_url - это путь к файлу
            print(f"Чтение из локального файла: {repo_url}")
            if repo_url.endswith('.gz'):
                with gzip.open(repo_url, 'rt', encoding='utf-8') as f:
                    return f.read()
            else:
                # На случай, если файл уже распакован
                with open(repo_url, 'r', encoding='utf-8') as f:
                    return f.read()

    except HTTPError as e:
        print(f"Ошибка HTTP при загрузке данных: {e.code} {e.reason}")
        sys.exit(1)
    except URLError as e:
        print(f"Ошибка URL: Не удалось подключиться. {e.reason}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"Ошибка: Локальный файл не найден: {repo_url}")
        sys.exit(1)
    except Exception as e:
        print(f"Неизвестная ошибка при получении данных: {e}")
        sys.exit(1)


def parse_dependencies(packages_data, target_package):
    print(f"Поиск зависимостей для пакета: {target_package}...")

    # Пакеты в файле разделены пустой строкой
    package_blocks = packages_data.split('\n\n')

    for block in package_blocks:
        lines = block.split('\n')

        # Используем простой парсинг ключ-значение
        package_info = {}
        for line in lines:
            if line.startswith('Package: '):
                package_info['Package'] = line.split(': ', 1)[1].strip()
            elif line.startswith('Depends: '):
                package_info['Depends'] = line.split(': ', 1)[1].strip()

        # Мы нашли нужный нам пакет?
        if package_info.get('Package') == target_package:
            if 'Depends' not in package_info:
                return []  # Пакет найден, но у него нет зависимостей

            deps_string = package_info['Depends']
            deps_list_raw = deps_string.split(',')
            final_dependencies = [dep for dep in deps_list_raw]

            return final_dependencies

    # Если мы прошли весь цикл и не нашли пакет
    return None


def main():
    # 1. Загрузка конфигурации
    # (Мы больше не предлагаем выбор, просто используем стандартный config.toml)
    config = load_config("config.toml")

    # 2. Валидация
    errors = validate_config(config)
    if errors:
        print("Ошибки конфигурации:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)

    # 3. Получение данных (Этап 2)
    packages_data_str = get_packages_data(config)

    if not packages_data_str:
        print("Не удалось получить данные о пакетах.")
        sys.exit(1)

    # 4. Парсинг зависимостей (Этап 2)
    target = config['package_name']
    dependencies = parse_dependencies(packages_data_str, target)

    # 5. Вывод (Требование 3 Этапа 2)
    if dependencies is None:
        print(f"\nОшибка: Пакет '{target}' не найден в репозитории.")
    elif not dependencies:
        print(f"\nПакет '{target}' найден, но не имеет прямых зависимостей.")
    else:
        print(f"\nПрямые зависимости для пакета '{target}':")
        for dep in dependencies:
            print(f"  - {dep}")


if __name__ == "__main__":
    main()