import toml
import sys
import os
import gzip
import re
from urllib import request as url_request
from urllib.error import URLError, HTTPError
from collections import defaultdict, deque
import argparse


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
    """Валидация параметров конфигурации"""
    errors = []

    if not config.get('package_name'):
        errors.append("Имя пакета (package_name) не может быть пустым")

    valid_modes = ['local', 'remote', 'test']
    mode = config.get('working_mode')
    if mode not in valid_modes:
        errors.append(f"Режим работы (working_mode) должен быть одним из: {', '.join(valid_modes)}")

    repo_url = config.get('repository_url', '')
    if not repo_url:
        errors.append("URL репозитория (repository_url) не может быть пустым")

    if mode == 'remote':
        if not repo_url.startswith(('http://', 'https://')):
            errors.append("Для 'remote' режима repository_url должен начинаться с http:// или https://")
        if not config.get('distribution'):
            errors.append("distribution (напр. 'jammy') обязателен для 'remote' режима")
        if not config.get('component'):
            errors.append("component (напр. 'main') обязателен для 'remote' режима")
        if not config.get('architecture'):
            errors.append("architecture (напр. 'amd64') обязателен для 'remote' режима")

    elif mode == 'local':
        if repo_url.startswith(('http://', 'https://')):
            errors.append("Для 'local' режима repository_url должен быть локальным путем, а не URL")

    depth = config.get('max_depth', 1)
    if not isinstance(depth, int) or depth < 1 or depth > 20:
        errors.append("Глубина анализа (max_depth) должна быть целым числом от 1 до 20")

    filter_str = config.get('filter_substring', '')
    if not isinstance(filter_str, str):
        errors.append("Подстрока фильтра (filter_substring) должна быть строкой")

    return errors


def get_packages_data(config):
    """Загружает и распаковывает файл Packages.gz"""
    mode = config['working_mode']
    repo_url = config['repository_url']

    print(f"Режим работы: {mode}. Получение данных...")

    try:
        if mode == 'remote':
            full_url = (
                f"{repo_url}/dists/{config['distribution']}/"
                f"{config['component']}/binary-{config['architecture']}/Packages.gz"
            )
            print(f"Загрузка из: {full_url}")

            with url_request.urlopen(full_url) as response:
                with gzip.GzipFile(fileobj=response) as gzip_file:
                    return gzip_file.read().decode('utf-8')

        elif mode == 'local' or mode == 'test':
            print(f"Чтение из локального файла: {repo_url}")
            if repo_url.endswith('.gz'):
                with gzip.open(repo_url, 'rt', encoding='utf-8') as f:
                    return f.read()
            else:
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


def parse_package_dependencies(block):
    """Парсит зависимости из блока пакета"""
    package_info = {}
    lines = block.split('\n')

    for line in lines:
        if line.startswith('Package: '):
            package_info['Package'] = line.split(': ', 1)[1].strip()
        elif line.startswith('Depends: '):
            deps_string = line.split(': ', 1)[1].strip()
            # Упрощенный парсинг зависимостей (игнорируем версии)
            dependencies = []
            for dep in deps_string.split(','):
                dep = dep.strip()
                # Убираем информацию о версии (все что в скобках)
                dep = re.sub(r'\([^)]*\)', '', dep).strip()
                # Убираем альтернативы (все что после |)
                dep = dep.split('|')[0].strip()
                if dep:
                    dependencies.append(dep)
            package_info['Depends'] = dependencies

    return package_info


def build_dependency_graph(packages_data):
    """Строит граф зависимостей из всех пакетов"""
    graph = defaultdict(list)
    package_blocks = packages_data.split('\n\n')

    for block in package_blocks:
        if not block.strip():
            continue

        package_info = parse_package_dependencies(block)
        if 'Package' in package_info:
            package_name = package_info['Package']
            dependencies = package_info.get('Depends', [])
            graph[package_name] = dependencies

    return graph


def parse_test_graph(file_path):
    """Парсит тестовый граф из файла"""
    graph = defaultdict(list)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '->' in line:
                    parts = line.split('->')
                    if len(parts) == 2:
                        source = parts[0].strip()
                        targets = [t.strip() for t in parts[1].split(',')]
                        graph[source] = targets
    except FileNotFoundError:
        print(f"Ошибка: Тестовый файл не найден: {file_path}")
        sys.exit(1)

    return graph


def bfs_dependencies_recursive(graph, start_package, max_depth, current_depth, visited, filter_substring, result):
    """Рекурсивный BFS для поиска зависимостей с учетом глубины и фильтра"""
    if current_depth > max_depth:
        return

    if start_package in visited:
        return

    visited.add(start_package)

    # Применяем фильтр
    if filter_substring and filter_substring in start_package:
        return

    if start_package not in graph:
        return

    for dependency in graph[start_package]:
        if dependency not in visited and (not filter_substring or filter_substring not in dependency):
            result[dependency] = current_depth
            bfs_dependencies_recursive(graph, dependency, max_depth, current_depth + 1, visited, filter_substring,
                                       result)


def get_transitive_dependencies(graph, start_package, max_depth=3, filter_substring=""):
    """Получает транзитивные зависимости с использованием BFS"""
    if start_package not in graph:
        return {}

    result = {}
    visited = set()

    bfs_dependencies_recursive(graph, start_package, max_depth, 1, visited, filter_substring, result)

    return result


def detect_cycles(graph):
    """Обнаруживает циклические зависимости в графе"""

    def dfs(node, path, visited, rec_stack, cycles):
        visited.add(node)
        rec_stack.add(node)
        path.append(node)

        for neighbor in graph.get(node, []):
            if neighbor not in graph:
                continue
            if neighbor not in visited:
                dfs(neighbor, path, visited, rec_stack, cycles)
            elif neighbor in rec_stack:
                # Найден цикл
                cycle_start = path.index(neighbor)
                cycle = path[cycle_start:]
                cycles.add(tuple(cycle))

        path.pop()
        rec_stack.remove(node)

    visited = set()
    cycles = set()

    for node in graph:
        if node not in visited:
            dfs(node, [], visited, set(), cycles)

    return cycles


def print_dependency_tree(dependencies, graph, start_package, indent=0):
    """Рекурсивно печатает дерево зависимостей"""
    for dep, depth in sorted(dependencies.items()):
        if depth == 1:
            print("  " * indent + f"├── {dep}")
            # Рекурсивно печатаем зависимости этой зависимости
            sub_deps = {k: v - 1 for k, v in dependencies.items() if v > 1}
            print_dependency_tree(sub_deps, graph, dep, indent + 1)


def main():
    parser = argparse.ArgumentParser(description='Анализатор зависимостей пакетов')
    parser.add_argument('--config', default='config.toml', help='Путь к файлу конфигурации')
    parser.add_argument('--package', help='Имя пакета для анализа (переопределяет config)')
    parser.add_argument('--depth', type=int, help='Максимальная глубина анализа (переопределяет config)')
    parser.add_argument('--filter', help='Подстрока для фильтрации пакетов (переопределяет config)')
    parser.add_argument('--test-file', help='Путь к тестовому файлу графа')

    args = parser.parse_args()

    # Загрузка конфигурации
    config = load_config(args.config)

    # Переопределение параметров из командной строки
    if args.package:
        config['package_name'] = args.package
    if args.depth:
        config['max_depth'] = args.depth
    if args.filter:
        config['filter_substring'] = args.filter

    # Валидация
    errors = validate_config(config)
    if errors:
        print("Ошибки конфигурации:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)

    # Режим тестирования
    if args.test_file or config.get('working_mode') == 'test':
        test_file = args.test_file or config.get('repository_url')
        if not test_file:
            print("Ошибка: Для тестового режима укажите --test-file или repository_url в config")
            sys.exit(1)

        print(f"Тестовый режим: загрузка графа из {test_file}")
        graph = parse_test_graph(test_file)
    else:
        # Режим работы с реальными пакетами
        packages_data_str = get_packages_data(config)
        if not packages_data_str:
            print("Не удалось получить данные о пакетах.")
            sys.exit(1)

        graph = build_dependency_graph(packages_data_str)

    target_package = config['package_name']
    max_depth = config.get('max_depth', 3)
    filter_substring = config.get('filter_substring', '')

    print(f"\nАнализ зависимостей для пакета: {target_package}")
    print(f"Максимальная глубина: {max_depth}")
    if filter_substring:
        print(f"Фильтр: исключаем пакеты содержащие '{filter_substring}'")

    # Проверка существования пакета
    if target_package not in graph:
        print(f"Ошибка: Пакет '{target_package}' не найден в графе")
        sys.exit(1)

    # Обнаружение циклических зависимостей
    cycles = detect_cycles(graph)
    if cycles:
        print("\n⚠️  Обнаружены циклические зависимости:")
        for cycle in cycles:
            print(f"  Цикл: {' -> '.join(cycle)} -> ...")
    else:
        print("\n✓ Циклические зависимости не обнаружены")

    # Получение транзитивных зависимостей
    print(f"\nТранзитивные зависимости (BFS с рекурсией):")
    dependencies = get_transitive_dependencies(graph, target_package, max_depth, filter_substring)

    if not dependencies:
        print(f"Пакет '{target_package}' не имеет транзитивных зависимостей")
    else:
        print(f"Найдено {len(dependencies)} зависимостей:")
        for dep, depth in sorted(dependencies.items(), key=lambda x: x[1]):
            print(f"  Глубина {depth}: {dep}")

        # Дополнительная информация
        print(f"\nДополнительная статистика:")
        print(
            f"  Прямые зависимости: {len([d for d in graph[target_package] if not filter_substring or filter_substring not in d])}")
        print(f"  Всего транзитивных зависимостей: {len(dependencies)}")

        # Поиск самого длинного пути
        if dependencies:
            max_depth_found = max(dependencies.values())
            deepest_packages = [pkg for pkg, depth in dependencies.items() if depth == max_depth_found]
            print(f"  Максимальная глубина зависимостей: {max_depth_found}")
            print(f"  Самые глубокие зависимости: {', '.join(deepest_packages)}")


if __name__ == "__main__":
    main()