
"""
Основной скрипт для парсинга компаний из нескольких источников
"""

import sys
import os
import logging
from typing import Dict, List
import pandas as pd
import hashlib

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем родительскую директорию в путь для импортов
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from src.listorg_parser import ListOrgParser
    from src.rusprofile_parser import RusprofileParser
    from src.fns_parser import FnsOpenDataParser
    from src.ruward_parser import RuwardParser  # Добавлен импорт парсера RUWARD
except ImportError:
    from listorg_parser import ListOrgParser
    from rusprofile_parser import RusprofileParser
    from fns_parser import FnsOpenDataParser
    from ruward_parser import RuwardParser  # Добавлен импорт парсера RUWARD


class ParserManager:
    """Менеджер для управления несколькими парсерами"""
    
    def __init__(self):
        self.parsers = {
            'list_org': ListOrgParser(),
            'rusprofile': RusprofileParser(),
            'fns_open_data': FnsOpenDataParser(),
            'ruward': RuwardParser()  # Добавлен парсер RUWARD
        }
    
    def run_parser(self, parser_name: str, search_queries: List[str], 
                   max_companies: int = 50) -> List[Dict]:
        """Запуск конкретного парсера"""
        if parser_name not in self.parsers:
            logger.error(f"Parser '{parser_name}' not found")
            return []
        
        parser = self.parsers[parser_name]
        logger.info(f"\n{'='*60}")
        logger.info(f"Запуск парсера: {parser_name}")
        logger.info(f"{'='*60}")
        
        # Для парсеров, которые не используют search_queries
        if parser_name in ['fns_open_data', 'ruward']:
            return parser.collect_companies(max_companies=max_companies)
        else:
            return parser.collect_companies(search_queries, max_companies)
    
    def run_all_parsers(self, search_queries: List[str], 
                        max_per_parser: int = 50) -> Dict[str, List[Dict]]:
        """Запуск всех парсеров"""
        results = {}
        
        for parser_name, parser in self.parsers.items():
            try:
                if parser_name in ['fns_open_data', 'ruward']:
                    # Для парсеров, которые не используют search_queries
                    logger.info(f"\nЗапуск парсера {parser_name}...")
                    companies = parser.collect_companies(max_companies=max_per_parser)
                else:
                    # Для парсеров, использующих search_queries
                    companies = parser.collect_companies(search_queries, max_per_parser)
                
                results[parser_name] = companies
                
                # Сохраняем сырые данные от каждого парсера
                if companies:
                    filename_map = {
                        'fns_open_data': 'data/fns_companies.csv',
                        'list_org': 'data/listorg_companies.csv',
                        'rusprofile': 'data/rusprofile_companies.csv',
                        'ruward': 'data/ruward_companies.csv'  # Добавлено сохранение для RUWARD
                    }
                    
                    if parser_name in filename_map:
                        parser.save_to_csv(companies, filename_map[parser_name])
                        
            except Exception as e:
                logger.error(f"Ошибка в парсере {parser_name}: {e}")
                results[parser_name] = []
        
        return results
    
    def merge_results(self, all_results: Dict[str, List[Dict]]) -> List[Dict]:
        """Объединение результатов от всех парсеров с дедупликацией"""
        merged_companies = []
        processed_ids = set()
        
        for parser_name, companies in all_results.items():
            logger.info(f"Обработка результатов от {parser_name}: {len(companies)} компаний")
            for company in companies:
                # Пробуем получить уникальный идентификатор
                company_id = self._get_company_id(company, parser_name)
                
                if company_id and company_id not in processed_ids:
                    processed_ids.add(company_id)
                    merged_companies.append(company)
                elif not company_id:
                    # Если нет ИНН, создаем уникальный ID
                    import hashlib
                    unique_id = hashlib.md5(str(company).encode()).hexdigest()[:10]
                    if unique_id not in processed_ids:
                        processed_ids.add(unique_id)
                        merged_companies.append(company)
        
        return merged_companies
    
    def _get_company_id(self, company: Dict, parser_name: str) -> str:
        """Получение уникального идентификатора компании"""
        # Сначала пробуем получить ИНН
        inn = company.get('inn')
        if inn:
            return f"inn_{inn}"
        
        # Для RUWARD используем название и сайт
        if parser_name == 'ruward':
            name = company.get('name', '')
            site = company.get('site', '')
            if name and site:
                return f"ruward_{hashlib.md5(f'{name}_{site}'.encode()).hexdigest()[:10]}"
        
        return None
    
    def save_merged_results(self, companies: List[Dict], 
                           filename: str = 'data/companies.csv') -> str:
        """Сохранение объединенных результатов в основной файл"""
        if not companies:
            logger.warning("No companies to save")
            return ""
        
        import pandas as pd
        
        # Создаем DataFrame
        df = pd.DataFrame(companies)
        
        # Определяем общие колонки для всех источников
        all_columns = set()
        for company in companies:
            all_columns.update(company.keys())
        
        # Сортируем колонки для удобства
        priority_columns = ['inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 
                          'source', 'category', 'site', 'rating_position', 'rating_category']
        
        # Собираем все колонки
        ordered_columns = []
        for col in priority_columns:
            if col in all_columns:
                ordered_columns.append(col)
                all_columns.remove(col)
        
        # Добавляем остальные колонки
        ordered_columns.extend(sorted(all_columns))
        
        # Создаем DataFrame с упорядоченными колонками
        df = df.reindex(columns=ordered_columns)
        
        # Преобразуем числовые колонки
        numeric_columns = ['revenue', 'employees', 'rating_position', 'revenue_year', 'foundation_year']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Создаем директорию
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Сохраняем
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        logger.info(f"\nОбъединенный файл сохранен: {filename}")
        return filename
    
    def run_interactive(self):
        """Интерактивный запуск парсеров"""
        print("\n" + "="*60)
        print("ДОСТУПНЫЕ ПАРСЕРЫ:")
        print("="*60)
        print("1. ListOrg (list-org.com)")
        print("2. RusProfile (rusprofile.ru)")
        print("3. FNS Open Data (data.nalog.ru)")
        print("4. RUWARD (ruward.ru)")  # Добавлен RUWARD
        print("5. Все парсеры")
        print("="*60)
        
        choice = input("\nВыберите парсер (1-5): ").strip()
        
        search_queries = [
            "BTL агентство",
            "ивент агентство", 
            "сувенирная продукция",
            "рекламное агентство полный цикл",
            "коммуникационная группа",
            "мерчандайзинг",
            "промо акции",
            "бренд активация",
            "маркетинговые услуги",
            "организация мероприятий"
        ]
        
        max_companies = 50
        
        if choice == "1":
            companies = self.run_parser('list_org', search_queries, max_companies)
            if companies:
                self.parsers['list_org'].save_to_csv(companies, 'data/listorg_companies.csv')
        
        elif choice == "2":
            companies = self.run_parser('rusprofile', search_queries, max_companies)
            if companies:
                self.parsers['rusprofile'].save_to_csv(companies, 'data/rusprofile_companies.csv')
        
        elif choice == "3":
            # Запуск только FNS парсера
            print("\nЗапуск парсера данных ФНС...")
            companies = self.parsers['fns_open_data'].collect_companies(max_companies=max_companies)
            if companies:
                self.parsers['fns_open_data'].save_to_csv(companies, 'data/fns_companies.csv')
                print(f"Найдено {len(companies)} компаний в данных ФНС")
        
        elif choice == "4":
            # Запуск только RUWARD парсера
            print("\nЗапуск парсера RUWARD...")
            companies = self.parsers['ruward'].collect_companies(max_companies=max_companies)
            if companies:
                self.parsers['ruward'].save_to_csv(companies, 'data/ruward_companies.csv')
                print(f"Найдено {len(companies)} компаний в рейтингах RUWARD")
                
                # Показываем пример компании
                if companies:
                    print("\nПример компании из RUWARD:")
                    sample = companies[0]
                    for key in ['name', 'category', 'revenue', 'rating_position', 'segment_tag']:
                        if key in sample:
                            print(f"  {key}: {sample[key]}")
        
        elif choice == "5":
            # Запуск всех парсеров
            all_results = self.run_all_parsers(search_queries, max_per_parser=30)
            merged_companies = self.merge_results(all_results)
            self.save_merged_results(merged_companies, 'data/all_companies.csv')
            print_statistics(merged_companies, all_results)
        
        else:
            print("Неверный выбор")


def main():
    """Основная функция"""
    print("=" * 60)
    print("ПАРСЕР BTL И МАРКЕТИНГОВЫХ АГЕНТСТВ (Мульти-источник)")
    print("=" * 60)
    
    # Создаем менеджер парсеров
    manager = ParserManager()
    
    # Запросы для поиска (используются всеми парсерами кроме FNS и RUWARD)
    search_queries = [
        "BTL агентство",
        "ивент агентство", 
        "сувенирная продукция",
        "рекламное агентство полный цикл",
        "коммуникационная группа",
        "мерчандайзинг",
        "промо акции",
        "бренд активация",
        "маркетинговые услуги",
        "организация мероприятий"
    ]
    
    try:
        print(f"\nДоступно 4 источника данных:")
        print(f"1. ListOrg - поиск по запросам")
        print(f"2. RusProfile - поиск по запросам") 
        print(f"3. FNS Open Data - открытые данные ФНС")
        print(f"4. RUWARD - рейтинги агентств (digital, PR, BTL, event)")
        
        mode = input("\nВыберите режим (1-интерактивный, 2-запустить все): ").strip()
        
        if mode == "1":
            # Интерактивный режим
            manager.run_interactive()
        elif mode == "2":
            # Автоматический запуск всех парсеров
            print(f"\nЗапускаем все парсеры...")
            
            all_results = manager.run_all_parsers(search_queries, max_per_parser=30)
            
            # Объединяем результаты
            merged_companies = manager.merge_results(all_results)
            
            # Сохраняем объединенные результаты
            main_file = manager.save_merged_results(merged_companies, 'data/companies.csv')
            
            # Выводим статистику
            print_statistics(merged_companies, all_results)
        else:
            print("Запускаем все парсеры по умолчанию...")
            all_results = manager.run_all_parsers(search_queries, max_per_parser=30)
            merged_companies = manager.merge_results(all_results)
            main_file = manager.save_merged_results(merged_companies, 'data/companies.csv')
            print_statistics(merged_companies, all_results)
        
    except KeyboardInterrupt:
        print("\n\nПарсинг прерван пользователем")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}", exc_info=True)
        sys.exit(1)


def print_statistics(merged_companies: List[Dict], all_results: Dict[str, List[Dict]]):
    """Вывод статистики по результатам"""
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ ПАРСИНГА")
    print("=" * 60)
    
    # Статистика по источникам
    print("\nСобрано по источникам:")
    total_all = 0
    for source, companies in all_results.items():
        count = len(companies)
        total_all += count
        source_name = {
            'list_org': 'ListOrg',
            'rusprofile': 'RusProfile', 
            'fns_open_data': 'FNS Open Data',
            'ruward': 'RUWARD'
        }.get(source, source)
        print(f"  {source_name}: {count} компаний")
    
    print(f"Всего собрано: {total_all} компаний")
    print(f"После дедупликации: {len(merged_companies)} уникальных компаний")
    
    if merged_companies:
        # Статистика по сегментам
        segments = {}
        for company in merged_companies:
            if company.get('segment_tag'):
                tag = company['segment_tag']
                if isinstance(tag, str):
                    for segment in tag.split('|'):
                        segments[segment] = segments.get(segment, 0) + 1
        
        print("\nРаспределение по сегментам:")
        for segment, count in sorted(segments.items(), key=lambda x: x[1], reverse=True):
            print(f"  {segment}: {count} компаний")
        
        # Статистика по выручке
        revenues = [c['revenue'] for c in merged_companies if c.get('revenue')]
        if revenues:
            print(f"\nСтатистика по выручке:")
            print(f"  Компаний с выручкой: {len(revenues)}")
            if revenues:
                print(f"  Минимальная: {min(revenues):,.0f} ₽")
                print(f"  Максимальная: {max(revenues):,.0f} ₽")
                print(f"  Средняя: {sum(revenues)/len(revenues):,.0f} ₽")
        
        # Статистика по рейтинговым позициям (для RUWARD)
        ratings = [c['rating_position'] for c in merged_companies 
                  if c.get('rating_position') and c.get('source') == 'ruward']
        if ratings:
            print(f"\nСтатистика по рейтингам RUWARD:")
            print(f"  Агентств в рейтингах: {len(ratings)}")
            print(f"  Лучшая позиция: #{min(ratings)}")
            print(f"  Средняя позиция: #{sum(ratings)/len(ratings):.1f}")
        
        # Статистика по источникам в объединенном файле
        sources = {}
        categories = {}
        for company in merged_companies:
            source = company.get('source', 'unknown')
            sources[source] = sources.get(source, 0) + 1
            
            # Для RUWARD собираем категории
            if source == 'ruward' and company.get('category'):
                category = company['category']
                categories[category] = categories.get(category, 0) + 1
        
        print("\nИсточники в объединенном файле:")
        for source, count in sorted(sources.items()):
            source_name = {
                'list_org': 'ListOrg',
                'rusprofile': 'RusProfile', 
                'fns_open_data': 'FNS Open Data',
                'ruward': 'RUWARD'
            }.get(source, source)
            print(f"  {source_name}: {count} компаний")
        
        # Статистика по категориям RUWARD
        if categories:
            print("\nКатегории агентств из RUWARD:")
            for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
                print(f"  {category}: {count} агентств")
        
        # Топ компаний по выручке
        if 'revenue' in merged_companies[0] and 'name' in merged_companies[0]:
            sorted_by_revenue = sorted([c for c in merged_companies if c.get('revenue')], 
                                      key=lambda x: x['revenue'], reverse=True)
            print(f"\nТоп-5 компаний по выручке:")
            for i, company in enumerate(sorted_by_revenue[:5], 1):
                name = company.get('name', 'Unknown')
                revenue = company.get('revenue', 0)
                source = company.get('source', 'unknown')
                source_name = {
                    'list_org': 'ListOrg',
                    'rusprofile': 'RusProfile', 
                    'fns_open_data': 'FNS',
                    'ruward': 'RUWARD'
                }.get(source, source)
                print(f"  {i}. {name} - {revenue:,.0f} ₽ ({source_name})")
        
        # Сохраняем подробный отчет
        save_detailed_report(merged_companies, all_results)


def save_detailed_report(merged_companies: List[Dict], all_results: Dict[str, List[Dict]]):
    """Сохранение подробного отчета"""
    report_lines = []
    
    report_lines.append("=" * 60)
    report_lines.append("ПОДРОБНЫЙ ОТЧЕТ О ПАРСИНГЕ")
    report_lines.append("=" * 60)
    report_lines.append(f"Дата: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Всего уникальных компаний: {len(merged_companies)}")
    
    report_lines.append("\nСтатистика по источникам:")
    total_all = 0
    for source, companies in all_results.items():
        count = len(companies)
        total_all += count
        source_name = {
            'list_org': 'ListOrg',
            'rusprofile': 'RusProfile', 
            'fns_open_data': 'FNS Open Data',
            'ruward': 'RUWARD'
        }.get(source, source)
        report_lines.append(f"  {source_name}: {count} компаний")
    
    report_lines.append(f"Всего собрано: {total_all} компаний")
    
    # Детальная информация по компаниям
    report_lines.append("\n" + "=" * 60)
    report_lines.append("ДЕТАЛЬНАЯ ИНФОРМАЦИЯ ПО КОМПАНИЯМ")
    report_lines.append("=" * 60)
    
    for i, company in enumerate(merged_companies, 1):
        report_lines.append(f"\n{i}. {company.get('name', 'Unknown')}")
        report_lines.append(f"   Источник: {company.get('source', 'unknown')}")
        
        if company.get('revenue'):
            report_lines.append(f"   Выручка: {company['revenue']:,.0f} ₽ ({company.get('revenue_year', 'N/A')} г.)")
        
        if company.get('segment_tag'):
            report_lines.append(f"   Сегмент: {company['segment_tag']}")
        
        if company.get('category'):
            report_lines.append(f"   Категория: {company['category']}")
        
        if company.get('rating_position'):
            report_lines.append(f"   Позиция в рейтинге: #{company['rating_position']}")
        
        if company.get('site'):
            report_lines.append(f"   Сайт: {company['site']}")
        
        if company.get('employees'):
            report_lines.append(f"   Сотрудники: {company['employees']}")
    
    # Сохраняем отчет
    os.makedirs('data', exist_ok=True)
    with open('data/detailed_report.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\nПодробный отчет сохранен: data/detailed_report.txt")


if __name__ == "__main__":
    main()
