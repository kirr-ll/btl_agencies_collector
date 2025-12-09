#!/usr/bin/env python3
"""
Основной скрипт для парсинга компаний из нескольких источников
"""

import sys
import os
import logging
from typing import Dict, List
import pandas as pd

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
    from src.fns_parser import FnsOpenDataParser  # Изменено с fn_parser на fns_parser
except ImportError:
    from listorg_parser import ListOrgParser
    from rusprofile_parser import RusprofileParser
    from src.fns_parser import FnsOpenDataParser  # Изменено с fn_parser на fns_parser


class ParserManager:
    """Менеджер для управления несколькими парсерами"""
    
    def __init__(self):
        self.parsers = {
            'list_org': ListOrgParser(),
            'rusprofile': RusprofileParser(),
            'fns_open_data': FnsOpenDataParser()  # Добавлен парсер ФНС
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
        
        # Для FNS парсера не нужны search_queries, он работает с датасетами
        if parser_name == 'fns_open_data':
            return parser.collect_companies(max_companies=max_companies)
        else:
            return parser.collect_companies(search_queries, max_companies)
    
    def run_all_parsers(self, search_queries: List[str], 
                        max_per_parser: int = 50) -> Dict[str, List[Dict]]:
        """Запуск всех парсеров"""
        results = {}
        
        for parser_name, parser in self.parsers.items():
            try:
                if parser_name == 'fns_open_data':
                    # Для FNS используем специальный метод
                    logger.info(f"\nЗапуск парсера ФНС...")
                    companies = parser.collect_companies(max_companies=max_per_parser)
                else:
                    # Для остальных парсеров используем search_queries
                    companies = parser.collect_companies(search_queries, max_per_parser)
                
                results[parser_name] = companies
                
                # Сохраняем сырые данные от каждого парсера
                if companies:
                    if parser_name == 'fns_open_data':
                        parser.save_to_csv(companies, 'data/fns_companies.csv')
                    elif parser_name == 'list_org':
                        parser.save_to_csv(companies, 'data/listorg_companies.csv')
                    elif parser_name == 'rusprofile':
                        parser.save_to_csv(companies, 'data/rusprofile_companies.csv')
                        
            except Exception as e:
                logger.error(f"Ошибка в парсере {parser_name}: {e}")
                results[parser_name] = []
        
        return results
    
    def merge_results(self, all_results: Dict[str, List[Dict]]) -> List[Dict]:
        """Объединение результатов от всех парсеров с дедупликацией по ИНН"""
        merged_companies = []
        processed_inns = set()
        
        for parser_name, companies in all_results.items():
            logger.info(f"Обработка результатов от {parser_name}: {len(companies)} компаний")
            for company in companies:
                inn = company.get('inn')
                if inn and inn not in processed_inns:
                    processed_inns.add(inn)
                    merged_companies.append(company)
                elif not inn:
                    # Если нет ИНН, добавляем с уникальным ID
                    import hashlib
                    company_id = hashlib.md5(str(company).encode()).hexdigest()[:10]
                    if company_id not in processed_inns:
                        processed_inns.add(company_id)
                        merged_companies.append(company)
        
        return merged_companies
    
    def save_merged_results(self, companies: List[Dict], 
                           filename: str = 'data/companies.csv') -> str:
        """Сохранение объединенных результатов в основной файл"""
        if not companies:
            logger.warning("No companies to save")
            return ""
        
        import pandas as pd
        
        # Создаем DataFrame
        df = pd.DataFrame(companies)
        
        # Обязательные колонки
        required_columns = ['inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 'source']
        optional_columns = ['okved_main', 'employees', 'site', 'description', 
                          'region', 'contacts', 'rating_ref']
        
        all_columns = required_columns + optional_columns
        
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        
        df = df[all_columns]
        
        # Форматирование
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        df['employees'] = pd.to_numeric(df['employees'], errors='coerce')
        
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
        print("4. Все парсеры")
        print("="*60)
        
        choice = input("\nВыберите парсер (1-4): ").strip()
        
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
    
    # Запросы для поиска (используются всеми парсерами кроме FNS)
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
        print(f"\nДоступно 3 источника данных:")
        print(f"1. ListOrg - поиск по запросам")
        print(f"2. RusProfile - поиск по запросам") 
        print(f"3. FNS Open Data - открытые данные ФНС (не требует запросов)")
        
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
            'fns_open_data': 'FNS Open Data'
        }.get(source, source)
        print(f"  {source_name}: {count} компаний")
    
    print(f"Всего собрано: {total_all} компаний")
    print(f"После дедупликации: {len(merged_companies)} уникальных компаний")
    
    if merged_companies:
        # Статистика по сегментам
        segments = {}
        for company in merged_companies:
            if company.get('segment_tag'):
                for segment in company['segment_tag'].split('|'):
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
        
        # Статистика по источникам в объединенном файле
        sources = {}
        for company in merged_companies:
            source = company.get('source', 'unknown')
            sources[source] = sources.get(source, 0) + 1
        
        print("\nИсточники в объединенном файле:")
        for source, count in sorted(sources.items()):
            source_name = {
                'list_org': 'ListOrg',
                'rusprofile': 'RusProfile', 
                'fns_open_data': 'FNS Open Data'
            }.get(source, source)
            print(f"  {source_name}: {count} компаний")
        
        # Сохраняем краткий отчет
        with open('data/parsing_report.txt', 'w', encoding='utf-8') as f:
            f.write(f"Отчет о парсинге\n")
            f.write(f"Дата: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Всего уникальных компаний: {len(merged_companies)}\n")
            f.write("\nПо источникам:\n")
            for source, companies in all_results.items():
                source_name = {
                    'list_org': 'ListOrg',
                    'rusprofile': 'RusProfile', 
                    'fns_open_data': 'FNS Open Data'
                }.get(source, source)
                f.write(f"  {source_name}: {len(companies)} компаний\n")
            f.write("\nПо сегментам:\n")
            for segment, count in sorted(segments.items(), key=lambda x: x[1], reverse=True):
                f.write(f"  {segment}: {count} компаний\n")


if __name__ == "__main__":
    main()