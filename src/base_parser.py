import pandas as pd
import os
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class BaseParser(ABC):
    """Базовый класс для всех парсеров"""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.companies = []
        
    @abstractmethod
    def search_companies(self, query: str, pages: int = 2) -> List[str]:
        """Поиск компаний по запросу"""
        pass
    
    @abstractmethod
    def parse_company_page(self, url_path: str) -> Optional[Dict]:
        """Парсинг страницы компании"""
        pass
    
    @abstractmethod
    def collect_companies(self, search_queries: List[str], max_companies: int = 100) -> List[Dict]:
        """Сбор компаний"""
        pass
    
    def save_to_csv(self, companies: List[Dict], filename: str = None) -> str:
        """Сохранение данных в CSV"""
        if not companies:
            logger.warning(f"[{self.source_name}] No data to save")
            return ""
        
        # Если имя файла не указано, генерируем автоматически
        if not filename:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/raw/{self.source_name}_companies_{timestamp}.csv"
        
        # Если нужно сохранить в папку raw с префиксом
        if not filename.startswith('data/raw/'):
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/raw/{self.source_name}_companies_{timestamp}.csv"
        
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
        
        # Форматирование числовых полей
        if 'revenue' in df.columns:
            df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        
        if 'employees' in df.columns:
            df['employees'] = pd.to_numeric(df['employees'], errors='coerce')
        
        # Создаем директорию если её нет
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Сохраняем
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        logger.info(f"[{self.source_name}] Saved {len(df)} companies to {filename}")
        return filename
    
    # Общие методы для всех парсеров
    def _is_russian_company(self, company_data: Dict) -> bool:
        """Проверка что компания российская"""
        inn = company_data.get('inn')
        if inn:
            inn_str = str(inn)
            if (len(inn_str) == 10 and inn_str.isdigit()) or (len(inn_str) == 12 and inn_str.isdigit()):
                return True
        
        region = str(company_data.get('region', '')).lower()
        russian_indicators = ['россия', 'рф', 'москва', 'санкт-петербург', 'область', 'край', 'республика']
        
        if any(indicator in region for indicator in russian_indicators):
            return True
        
        return True  # По умолчанию считаем российской
    
    def _is_relevant_profile(self, company_data: Dict) -> bool:
        """Проверка релевантного профиля"""
        # ... общая логика проверки профиля ...
        pass
    
    def _extract_revenue(self, text_lower: str) -> Optional[int]:
        """Извлечение выручки ≥ 200 млн рублей"""
        # ... общая логика извлечения выручки ...
        pass