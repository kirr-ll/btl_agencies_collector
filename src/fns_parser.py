import requests
import pandas as pd
import re
import os
import zipfile
import json
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional, Any
import random
from datetime import datetime
from fake_useragent import UserAgent
import logging
import io
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Импортируем BaseParser
try:
    from src.base_parser import BaseParser
except ImportError:
    from base_parser import BaseParser


class FnsOpenDataParser(BaseParser):
    """Парсер для данных ФНС России (Федеральной Налоговой Службы)"""
    
    def __init__(self):
        super().__init__('fns_open_data')
        self.base_url = "https://service.nalog.ru"
        self.open_data_url = "https://data.nalog.ru"
        self.session = requests.Session()
        self.ua = UserAgent()
        self._update_headers()
        
    def _update_headers(self):
        """Обновление заголовков с случайным User-Agent"""
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'application/json, text/html, application/xhtml+xml, application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://data.nalog.ru/',
            'Origin': 'https://data.nalog.ru',
        })
    
    def _make_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Безопасный HTTP запрос"""
        for attempt in range(max_retries):
            try:
                self._update_headers()
                response = self.session.get(
                    url, 
                    timeout=(15, 30), 
                    allow_redirects=True,
                    stream=True if url.endswith('.zip') else False
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code in [429, 502, 503]:
                    wait_time = (attempt + 1) * 10
                    logger.debug(f"Server error {response.status_code}. Waiting {wait_time}s...")
                    import time
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.debug(f"Request error {url}: {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(random.uniform(3, 7))
        
        return None
    
    # Реализация абстрактных методов из BaseParser
    def search_companies(self, query: str, pages: int = 2) -> List[str]:
        """
        Поиск компаний по запросу.
        Для FNS Open Data это не основной метод, поэтому возвращаем пустой список
        """
        logger.info(f"FNS parser doesn't support search by queries. Query: '{query}'")
        return []
    
    def parse_company_page(self, url_path: str) -> Optional[Dict]:
        """
        Парсинг страницы компании.
        Для FNS Open Data парсим конкретный URL с данными
        """
        try:
            # Проверяем, не является ли это URL датасета
            if 'opendata' in url_path or 'data.nalog' in url_path:
                logger.info(f"Parsing FNS dataset: {url_path}")
                companies = self.collect_companies(url_path, max_companies=10)
                return companies[0] if companies else None
            
            # Иначе пытаемся получить данные по ИНН
            inn_match = re.search(r'(\d{10}|\d{12})', url_path)
            if inn_match:
                inn = inn_match.group(1)
                return self._get_company_by_inn(inn)
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing FNS page {url_path}: {e}")
            return None
    
    def _get_company_by_inn(self, inn: str) -> Optional[Dict]:
        """Получение данных компании по ИНН"""
        # Заглушка - в реальном проекте нужно реализовать API запрос к ФНС
        logger.info(f"Getting company by INN: {inn} (not implemented)")
        return None
    
    def _download_and_extract_zip(self, url: str) -> Optional[str]:
        """Скачивание и распаковка ZIP файла"""
        try:
            logger.info(f"Downloading ZIP from: {url}")
            response = self._make_request(url)
            
            if not response:
                return None
            
            # Создаем временную директорию
            temp_dir = "temp_fns_data"
            os.makedirs(temp_dir, exist_ok=True)
            
            # Сохраняем ZIP файл
            zip_path = os.path.join(temp_dir, "data.zip")
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Распаковываем
            extracted_files = []
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                extracted_files = zip_ref.namelist()
            
            logger.info(f"Extracted {len(extracted_files)} files")
            
            # Ищем XML файлы
            xml_files = [f for f in extracted_files if f.endswith('.xml')]
            if xml_files:
                return os.path.join(temp_dir, xml_files[0])
            
            # Или JSON файлы
            json_files = [f for f in extracted_files if f.endswith('.json')]
            if json_files:
                return os.path.join(temp_dir, json_files[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error downloading/extracting ZIP: {e}")
            return None
    
    def _parse_xml_data(self, file_path: str) -> List[Dict]:
        """Парсинг XML данных ФНС"""
        companies = []
        
        try:
            logger.info(f"Parsing XML file: {file_path}")
            
            context = ET.iterparse(file_path, events=('start', 'end'))
            context = iter(context)
            event, root = next(context)
            
            company_data = {}
            current_element = None
            
            for event, elem in context:
                if event == 'start':
                    current_element = elem.tag
                    
                elif event == 'end':
                    if elem.tag.endswith('}Документ'):
                        if self._is_relevant_company(company_data):
                            companies.append(company_data.copy())
                        
                        company_data = {}
                        elem.clear()
                        root.clear()
                    
                    elif elem.tag.endswith('}ИНН'):
                        company_data['inn'] = elem.text
                    elif elem.tag.endswith('}НаимОрг'):
                        company_data['name'] = elem.text
                    elif elem.tag.endswith('}СумДоход'):
                        if elem.text and elem.text.isdigit():
                            company_data['revenue'] = int(elem.text)
                    elif elem.tag.endswith('}ОКВЭД'):
                        company_data['okved'] = elem.text
                    elif elem.tag.endswith('}Адрес'):
                        company_data['address'] = elem.text
                    
                    current_element = None
            
            logger.info(f"Parsed {len(companies)} companies from XML")
            return companies
            
        except Exception as e:
            logger.error(f"Error parsing XML: {e}")
            return []
    
    def _parse_json_data(self, file_path: str) -> List[Dict]:
        """Парсинг JSON данных ФНС"""
        companies = []
        
        try:
            logger.info(f"Parsing JSON file: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                for item in data:
                    company_data = self._extract_company_from_json(item)
                    if company_data and self._is_relevant_company(company_data):
                        companies.append(company_data)
            elif isinstance(data, dict) and 'data' in data:
                for item in data['data']:
                    company_data = self._extract_company_from_json(item)
                    if company_data and self._is_relevant_company(company_data):
                        companies.append(company_data)
            
            logger.info(f"Parsed {len(companies)} companies from JSON")
            return companies
            
        except Exception as e:
            logger.error(f"Error parsing JSON: {e}")
            return []
    
    def _extract_company_from_json(self, item: Dict) -> Optional[Dict]:
        """Извлечение данных компании из JSON объекта"""
        try:
            company_data = {
                'inn': None,
                'name': None,
                'revenue': None,
                'revenue_year': datetime.now().year - 1,
                'okved_main': None,
                'address': None,
                'employees': None,
                'region': None,
                'source': 'fns_open_data',
                'description': None,
                'site': None,
                'contacts': None,
                'rating_ref': None,
                'segment_tag': None
            }
            
            inn_keys = ['inn', 'ИНН', 'V_INN', 'innFl', 'innUl']
            name_keys = ['name', 'НаимОрг', 'V_NAIM_UL', 'fullName', 'shortName']
            revenue_keys = ['revenue', 'СумДоход', 'income', 'V_S_DOHOD', 's_dohod']
            okved_keys = ['okved', 'ОКВЭД', 'V_OKVED', 'mainOkved']
            address_keys = ['address', 'Адрес', 'V_ADRES', 'legalAddress']
            
            for key in inn_keys:
                if key in item and item[key]:
                    company_data['inn'] = str(item[key]).strip()
                    break
            
            for key in name_keys:
                if key in item and item[key]:
                    company_data['name'] = str(item[key]).strip()
                    break
            
            for key in revenue_keys:
                if key in item and item[key]:
                    try:
                        revenue = float(str(item[key]).replace(',', '.'))
                        company_data['revenue'] = int(revenue)
                    except:
                        pass
                    break
            
            for key in okved_keys:
                if key in item and item[key]:
                    company_data['okved_main'] = str(item[key]).strip()
                    break
            
            for key in address_keys:
                if key in item and item[key]:
                    address = str(item[key]).strip()
                    company_data['address'] = address
                    region = self._extract_region_from_address(address)
                    if region:
                        company_data['region'] = region
                    break
            
            if not company_data['inn'] or not company_data['name']:
                return None
            
            # Создаем rating_ref
            if company_data['inn']:
                company_data['rating_ref'] = f"fns_inn_{company_data['inn']}"
            
            # Определяем сегмент
            company_data['segment_tag'] = self._determine_segment(company_data)
            
            return company_data
            
        except Exception as e:
            logger.debug(f"Error extracting company from JSON: {e}")
            return None
    
    def _extract_region_from_address(self, address: str) -> Optional[str]:
        """Извлечение региона из адреса"""
        if not address:
            return None
        
        regions = [
            'Москва', 'Санкт-Петербург', 'Московская область', 'Ленинградская область',
            'Краснодарский край', 'Свердловская область', 'Ростовская область',
            'Республика Татарстан', 'Челябинская область', 'Новосибирская область'
        ]
        
        address_lower = address.lower()
        
        for region in regions:
            if region.lower() in address_lower:
                return region
        
        if 'москва' in address_lower or 'мск' in address_lower:
            return 'Москва'
        elif 'санкт-петербург' in address_lower or 'спб' in address_lower:
            return 'Санкт-Петербург'
        elif 'московская' in address_lower or 'мо обл' in address_lower:
            return 'Московская область'
        elif 'ленинградская' in address_lower or 'ленинград' in address_lower:
            return 'Ленинградская область'
        
        return None
    
    def _is_relevant_company(self, company_data: Dict) -> bool:
        """Фильтрация компаний по критериям"""
        
        inn = company_data.get('inn')
        if not inn:
            return False
        
        inn_str = str(inn)
        if not (len(inn_str) in [10, 12] and inn_str.isdigit()):
            return False
        
        revenue = company_data.get('revenue')
        if not revenue or revenue < 200000000:
            return False
        
        if not self._is_russian_company(company_data):
            return False
        
        if not self._is_relevant_profile(company_data):
            return False
        
        return True
    
    def _is_russian_company(self, company_data: Dict) -> bool:
        """Проверка что компания российская"""
        inn = company_data.get('inn')
        if inn:
            inn_str = str(inn)
            if len(inn_str) in [10, 12] and inn_str.isdigit():
                return True
        
        region = str(company_data.get('region', '')).lower()
        address = str(company_data.get('address', '')).lower()
        
        russian_indicators = [
            'россия', 'рф', 'ru', 'russia', 'российская федерация',
            'москва', 'санкт-петербург', 'спб', 'moscow', 'st. petersburg',
            'область', 'край', 'республика', 'респ.', 'автономный округ', 'ао',
            'г. ', 'город ', 'ул.', 'проспект', 'бульвар', 'проезд'
        ]
        
        text_to_check = f"{region} {address}"
        if any(indicator in text_to_check for indicator in russian_indicators):
            return True
        
        return True
    
    def _is_relevant_profile(self, company_data: Dict) -> bool:
        """Проверка релевантного профиля по названию и ОКВЭД"""
        name = str(company_data.get('name', '')).lower()
        okved = str(company_data.get('okved_main', ''))
        
        relevant_keywords = [
            'btl', 'промо', 'ивент', 'event', 'мерчандайзинг', 'мерчендайзинг',
            'бренд-актив', 'бренд активац', 'промоакц', 'живой маркетинг',
            'field marketing', 'live marketing', 'торговый маркетинг',
            'промоутер', 'промо-групп', 'активац', 'сэмплинг',
            'агентств', 'рекламн', 'маркетингов',
            'сувенир', 'подар', 'бизнес-сувенир', 'корпоративн', 'промопродукц',
            'полиграф', 'печат', 'тираж', 'календар', 'брендирован',
            'рекламн', 'премиальн', 'промо-сувенир',
            'полиграфия', 'полиграфическ', 'типограф', 'печатн',
            'коммуникац', 'комм груп', 'агентств', 'рекламн', 'маркетингов',
            'pr', 'public relations', 'digital', 'диджитал', 'креативн',
            'медиа', 'smm', 'контент', 'брендинг', 'стратеги',
            'полный цикл', 'full cycle', 'full-service', 'комплексн',
            'интегрирован', '360°', 'end-to-end', 'интегратор'
        ]
        
        if any(keyword in name for keyword in relevant_keywords):
            return True
        
        relevant_okveds = [
            '73.11', '73.12', '18.12', '74.10', '74.20', '90.03', '58.11', '58.19',
            '73.20', '74.30', '82.30'
        ]
        
        if any(okved.startswith(code) for code in relevant_okveds):
            return True
        
        return False
    
    def _determine_segment(self, company_data: Dict) -> str:
        """Определение сегмента компании"""
        name = str(company_data.get('name', '')).lower()
        okved = str(company_data.get('okved_main', ''))
        
        segments = []
        
        btl_keywords = ['btl', 'промо', 'ивент', 'event', 'мерчандайзинг', 'бренд-актив']
        if any(k in name for k in btl_keywords) or okved.startswith('73.11'):
            segments.append('BTL')
        
        souvenir_keywords = ['сувенир', 'подар', 'полиграф', 'печат', 'брендирован']
        if any(k in name for k in souvenir_keywords) or okved.startswith('18.12'):
            segments.append('SOUVENIR')
        
        full_cycle_keywords = ['полный цикл', 'full cycle', 'комплексн', 'интегрирован']
        if any(k in name for k in full_cycle_keywords):
            segments.append('FULL_CYCLE')
        
        comm_keywords = ['коммуникац', 'комм груп', 'агентств', 'рекламн', 'маркетингов', 'pr']
        if any(k in name for k in comm_keywords) or okved.startswith('73.12'):
            segments.append('COMM_GROUP')
        
        return '|'.join(segments) if segments else 'OTHER'
    
    def get_fns_datasets(self) -> List[Dict]:
        """Получение списка доступных наборов данных ФНС"""
        
        datasets = [
            {
                'name': 'ЕГРЮЛ - основные сведения',
                'url': 'https://data.nalog.ru/opendata/7707329152-egrul',
                'description': 'Основные сведения из ЕГРЮЛ',
                'format': 'zip/xml'
            },
            {
                'name': 'База данных ИНН-наименование',
                'url': 'https://data.nalog.ru/opendata/7707329152-inn',
                'description': 'Соответствие ИНН и наименований организаций',
                'format': 'zip/json'
            },
            {
                'name': 'Данные о доходах организаций',
                'url': 'https://data.nalog.ru/opendata/7707329152-dohod',
                'description': 'Данные о доходах (выручке) организаций',
                'format': 'zip/xml'
            },
            {
                'name': 'Реестр субъектов малого и среднего предпринимательства',
                'url': 'https://data.nalog.ru/opendata/7707329152-msp',
                'description': 'Сведения о субъектах МСП',
                'format': 'zip/json'
            }
        ]
        
        return datasets
    
    def collect_companies(self, dataset_url: str = None, max_companies: int = 100) -> List[Dict]:
        """Сбор компаний из данных ФНС"""
        
        all_companies = []
        processed_inns = set()
        
        logger.info(f"Starting collection from FNS Open Data (max {max_companies} companies)...")
        
        if not dataset_url:
            logger.info("Using default dataset...")
            datasets = self.get_fns_datasets()
            if len(datasets) >= 3:
                dataset_url = datasets[2]['url']  # Данные о доходах
        
        if dataset_url:
            logger.info(f"Downloading dataset from: {dataset_url}")
            
            extracted_file = self._download_and_extract_zip(dataset_url)
            
            if not extracted_file:
                logger.error("Failed to download or extract dataset")
                return []
            
            if extracted_file.endswith('.xml'):
                companies = self._parse_xml_data(extracted_file)
            elif extracted_file.endswith('.json'):
                companies = self._parse_json_data(extracted_file)
            else:
                logger.error(f"Unknown file format: {extracted_file}")
                return []
        else:
            logger.error("No dataset URL provided")
            return []
        
        for company in companies:
            if len(all_companies) >= max_companies:
                break
            
            if self._is_relevant_company(company):
                inn = company.get('inn')
                if inn and inn not in processed_inns:
                    processed_inns.add(inn)
                    all_companies.append(company)
                    logger.info(f"✓ {company['name']} - {company.get('revenue', 0):,} руб - {company.get('segment_tag', 'OTHER')}")
        
        if 'temp_fns_data' in os.listdir('.'):
            import shutil
            try:
                shutil.rmtree('temp_fns_data')
            except:
                pass
        
        logger.info(f"\nCollection complete. Found: {len(all_companies)} companies")
        return all_companies
    
    def save_to_csv(self, companies: List[Dict], filename: str = 'data/fns_companies.csv') -> str:
        """Сохранение данных в CSV"""
        if not companies:
            logger.warning("No data to save")
            return filename
        
        df = pd.DataFrame(companies)
        
        required_columns = ['inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 'source']
        optional_columns = ['okved_main', 'employees', 'site', 'description', 
                          'region', 'contacts', 'rating_ref', 'address']
        
        all_columns = required_columns + optional_columns
        
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        
        df = df[all_columns]
        
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        df['employees'] = pd.to_numeric(df['employees'], errors='coerce')
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        logger.info(f"File saved: {filename}")
        logger.info(f"Total companies: {len(df)}")
        logger.info(f"With revenue ≥200M: {df['revenue'].notna().sum()}")
        
        if 'segment_tag' in df.columns:
            logger.info("Segments distribution:")
            for segment, count in df['segment_tag'].value_counts().items():
                logger.info(f"  {segment}: {count}")
        
        return filename


def test_fns_parser():
    """Тест парсера FNS Open Data"""
    
    print("Testing FNS Open Data Parser")
    print("=" * 60)
    
    parser = FnsOpenDataParser()
    
    print("\n1. Available FNS datasets:")
    datasets = parser.get_fns_datasets()
    for i, dataset in enumerate(datasets, 1):
        print(f"{i}. {dataset['name']}")
        print(f"   URL: {dataset['url']}")
        print(f"   Desc: {dataset['description']}")
        print(f"   Format: {dataset['format']}")
        print()
    
    print("\n2. Testing abstract methods implementation:")
    print(f"   search_companies('test') returned: {len(parser.search_companies('test'))} results")
    print(f"   parse_company_page('test') returned: {parser.parse_company_page('test') is not None}")
    
    print("\n" + "=" * 60)
    print("Parser ready for use")


if __name__ == "__main__":
    test_fns_parser()