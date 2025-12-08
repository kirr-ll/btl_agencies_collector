#!/usr/bin/env python3
"""
Основной скрипт для парсинга компаний из нескольких источников
"""

import sys
import os
import logging
from typing import Dict, List

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
except ImportError:
    from listorg_parser import ListOrgParser
    from rusprofile_parser import RusprofileParser


class ParserManager:
    """Менеджер для управления несколькими парсерами"""
    
    def __init__(self):
        self.parsers = {
            'list_org': ListOrgParser(),
            'rusprofile': RusprofileParser(),
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
        
        return parser.collect_companies(search_queries, max_companies)
    
    def run_all_parsers(self, search_queries: List[str], 
                        max_per_parser: int = 50) -> Dict[str, List[Dict]]:
        """Запуск всех парсеров"""
        results = {}
        
        for parser_name, parser in self.parsers.items():
            companies = self.run_parser(parser_name, search_queries, max_per_parser)
            results[parser_name] = companies
            
            # Сохраняем сырые данные от каждого парсера
            if companies:
                parser.save_to_csv(companies)
        
        return results
    
    def merge_results(self, all_results: Dict[str, List[Dict]]) -> List[Dict]:
        """Объединение результатов от всех парсеров с дедупликацией по ИНН"""
        merged_companies = []
        processed_inns = set()
        
        for parser_name, companies in all_results.items():
            for company in companies:
                inn = company.get('inn')
                if inn and inn not in processed_inns:
                    processed_inns.add(inn)
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


def main():
    """Основная функция"""
    print("=" * 60)
    print("ПАРСЕР BTL И МАРКЕТИНГОВЫХ АГЕНТСТВ (Мульти-источник)")
    print("=" * 60)
    
    # Создаем менеджер парсеров
    manager = ParserManager()
    
    # Запросы для поиска
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
        print(f"\nЗапускаем парсеры по {len(search_queries)} запросам...")
        
        # Вариант 1: Запуск всех парсеров
        all_results = manager.run_all_parsers(search_queries, max_per_parser=50)
        
        # Вариант 2: Запуск только конкретного парсера
        # all_results = {'rusprofile': manager.run_parser('rusprofile', search_queries, 50)}
        
        # Объединяем результаты
        merged_companies = manager.merge_results(all_results)
        
        # Сохраняем объединенные результаты
        main_file = manager.save_merged_results(merged_companies, 'data/companies.csv')
        
        # Выводим статистику
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
    for source, companies in all_results.items():
        print(f"  {source}: {len(companies)} компаний")
    
    print(f"\nВсего уникальных компаний: {len(merged_companies)}")
    
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
            print(f"  Средняя: {sum(revenues)/len(revenues):,.0f} ₽")
            print(f"  Минимальная: {min(revenues):,.0f} ₽")
            print(f"  Максимальная: {max(revenues):,.0f} ₽")
            print(f"  Общая сумма: {sum(revenues):,.0f} ₽")
        
        # Статистика по источникам в объединенном файле
        sources = {}
        for company in merged_companies:
            source = company.get('source', 'unknown')
            sources[source] = sources.get(source, 0) + 1
        
        print("\nИсточники в объединенном файле:")
        for source, count in sorted(sources.items()):
            print(f"  {source}: {count} компаний")


if __name__ == "__main__":
    main()

