from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CompanyData:
    """Базовый класс для данных компании"""
    inn: str
    name: str
    source: str  # Название источника (rusprofile, listorg, rrar)


class BaseParser(ABC):
    """Абстрактный базовый класс для всех парсеров"""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
    
    @abstractmethod
    def get_company_data(self, inn: str) -> Optional[CompanyData]:
        """Основной метод получения данных компании"""
        pass
    
    @abstractmethod
    def batch_search(self, inn_list: list) -> list[CompanyData]:
        """Пакетный поиск компаний"""
        pass
    
    def validate_inn(self, inn: str) -> bool:
        """Валидация ИНН"""
        if not inn or not inn.isdigit():
            return False
        return len(inn) in (10, 12)