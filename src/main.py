#!/usr/bin/env python3
"""
Основной скрипт для парсинга компаний
"""

import sys
import os
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем родительскую директорию в путь для импортов
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from src.parser import ListOrgParser
except ImportError:
    from parser import ListOrgParser


def main():
    """Основная функция"""
    print("=" * 60)
    print("ПАРСЕР BTL И МАРКЕТИНГОВЫХ АГЕНТСТВ")
    print("=" * 60)
    
    # Создаем парсер
    parser = ListOrgParser()
    
    # Определяем запросы для поиска целевых компаний
    search_queries = [
        "BTL агентство",
        "ивент агентство",
        "мерчандайзинговая компания",
        "сувенирная продукция производство",
        "рекламное агентство полный цикл",
        "коммуникационная группа",
        "промо акции агентство",
        "бренд активация",
        "маркетинговые услуги агентство",
        "организация мероприятий",
        "производство рекламной продукции",
        "полиграфические услуги",
        "корпоративные подарки",
        "промоутерское агентство",
        "event management"
    ]
    
    try:
        # Собираем данные
        print(f"\nНачинаем сбор данных по {len(search_queries)} запросам...")
        print("Это может занять несколько минут...\n")
        
        companies = parser.collect_companies(search_queries, max_companies=100)
        
        # Сохраняем результат
        output_file = "data/companies.csv"
        saved_file = parser.save_to_csv(companies, output_file)
        
        # Выводим статистику
        print("\n" + "=" * 60)
        print("РЕЗУЛЬТАТЫ ПАРСИНГА:")
        print("=" * 60)
        print(f"Всего собрано компаний: {len(companies)}")
        
        if companies:
            # Статистика по сегментам
            segments = {}
            for company in companies:
                if company.get('segment_tag'):
                    for segment in company['segment_tag'].split('|'):
                        segments[segment] = segments.get(segment, 0) + 1
            
            print("\nРаспределение по сегментам:")
            for segment, count in sorted(segments.items(), key=lambda x: x[1], reverse=True):
                print(f"  {segment}: {count} компаний")
            
            # Статистика по регионам
            regions = {}
            for company in companies:
                region = company.get('region')
                if region:
                    # Упрощаем название региона
                    simple_region = region.split(',')[0] if ',' in region else region
                    simple_region = simple_region.split('г.')[-1].strip()
                    regions[simple_region] = regions.get(simple_region, 0) + 1
            
            print("\nТоп регионов:")
            for region, count in sorted(regions.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  {region}: {count} компаний")
            
            # Статистика по выручке
            revenues = [c.get('revenue', 0) for c in companies if c.get('revenue')]
            if revenues:
                avg_revenue = sum(revenues) / len(revenues)
                print(f"\nСредняя выручка: {avg_revenue:,.0f} ₽")
                print(f"Минимальная выручка: {min(revenues):,.0f} ₽")
                print(f"Максимальная выручка: {max(revenues):,.0f} ₽")
            
            print(f"\nФайл сохранен: {saved_file}")
            print("\nПримеры компаний:")
            for i, company in enumerate(companies[:3]):
                print(f"\n{i+1}. {company.get('name')}")
                print(f"   ИНН: {company.get('inn')}")
                print(f"   Выручка: {company.get('revenue', 0):,.0f} ₽")
                print(f"   Сегмент: {company.get('segment_tag')}")
                if company.get('region'):
                    print(f"   Регион: {company.get('region')}")
        
        print("\n" + "=" * 60)
        print("Парсинг завершен успешно!")
        
    except KeyboardInterrupt:
        print("\n\nПарсинг прерван пользователем")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}", exc_info=True)
        print(f"\nОшибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()