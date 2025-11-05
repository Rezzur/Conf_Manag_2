import toml
import sys
import os


def load_config(config_path):
    """Загрузка конфигурации из TOML файла"""
    try:
        with open(config_path, 'r') as f:
            return toml.load(f)
    except Exception as e:
        print(f"Ошибка загрузки конфигурации: {e}")
        sys.exit(1)


def validate_config(config):
    """Валидация параметров конфигурации"""
    errors = []

    # Проверка имени пакета
    if not config.get('package_name'):
        errors.append("Имя пакета не может быть пустым")

    # Проверка URL/пути репозитория
    repo_url = config.get('repository_url', '')
    if not repo_url:
        errors.append("URL репозитория не может быть пустым")
    elif not repo_url.startswith(('http://', 'https://', '/')):
        errors.append("Некорректный формат URL или пути репозитория")

    # Проверка режима работы
    valid_modes = ['local', 'remote', 'test']
    if config.get('working_mode') not in valid_modes:
        errors.append(f"Режим работы должен быть одним из: {', '.join(valid_modes)}")

    # Проверка глубины анализа
    depth = config.get('max_depth', 1)
    if not isinstance(depth, int) or depth < 1 or depth > 20:
        errors.append("Глубина анализа должна быть целым числом от 1 до 20")

    # Проверка фильтра
    filter_str = config.get('filter_substring', '')
    if not isinstance(filter_str, str):
        errors.append("Подстрока фильтра должна быть строкой")

    return errors


def main():
    # Загрузка конфигурации
    choice_config = input("1 - Стандартный.\n2 - Для показа ошибок\nВвод: ")
    if choice_config == '1':
        config = load_config("config.toml")
    if choice_config == '2':
        config = load_config("config_broken.toml")
    # Валидация
    errors = validate_config(config)
    if errors:
        print("Ошибки конфигурации:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)

    # Вывод параметров в формате ключ-значение
    print("Параметры конфигурации:")
    for key, value in config.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()