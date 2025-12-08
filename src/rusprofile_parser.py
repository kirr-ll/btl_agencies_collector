import requests
import re
import random
import time
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from fake_useragent import UserAgent

import logging
logger = logging.getLogger(__name__)

from .base_parser import BaseParser

class RusprofileParser(BaseParser):
    """Парсер для сайта rusprofile.ru"""
    
    def __init__(self):
        super().__init__('rusprofile')
        self.base_url = "https://www.rusprofile.ru"
        self.session = requests.Session()
        self.ua = UserAgent()
        self._update_headers()
        
    def _update_headers(self):
        """Обновление заголовков"""
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
    
    def search_companies(self, query: str, pages: int = 2) -> List[str]:
        """Поиск компаний по запросу на rusprofile"""
        company_links = []
        
        for page in range(1, pages + 1):
            try:
                encoded_query = requests.utils.quote(query)
                url = f"{self.base_url}/search?query={encoded_query}&page={page}"
                logger.info(f"[{self.source_name}] Search: '{query}' - page {page}")
                
                response = self.session.get(url, timeout=10)
                if response.status_code != 200:
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Ищем ссылки на компании (rusprofile использует /id/ для компаний)
                links = soup.select('a[href^="/id/"]')
                for link in links:
                    href = link.get('href')
                    if href and href not in company_links:
                        company_links.append(href)
                
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                logger.error(f"[{self.source_name}] Search error: {e}")
                continue
        
        return list(set(company_links))
    
    def parse_company_page(self, url_path: str) -> Optional[Dict]:
        """Парсинг страницы компании на rusprofile"""
        try:
            url = f"{self.base_url}{url_path}"
            logger.debug(f"[{self.source_name}] Parsing: {url}")
            
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.get_text()
            text_lower = text.lower()
            
            company_data = {
                'inn': None,
                'name': None,
                'revenue_year': 2023,
                'revenue': None,
                'segment_tag': None,
                'okved_main': None,
                'employees': None,
                'site': None,
                'description': None,
                'region': None,
                'contacts': None,
                'rating_ref': url_path,
                'source': self.source_name
            }
            
            # 1. Извлечение названия
            name_tag = soup.find('h1', class_='company-name')
            if name_tag:
                company_data['name'] = name_tag.get_text(strip=True)
            
            if not company_data['name']:
                return None
            
            # 2. Извлечение ИНН
            # На rusprofile ИНН обычно в заголовке или в информации о компании
            inn_patterns = [
                r'ИНН\s*[\:\-]?\s*(\d{10})',
                r'ИНН/КПП\s*[\:\-]?\s*(\d{10})',
                r'\b(\d{10})\b.*ИНН',
            ]
            
            for pattern in inn_patterns:
                match = re.search(pattern, text)
                if match:
                    company_data['inn'] = match.group(1)
                    break
            
            if not company_data['inn']:
                # Альтернативный поиск ИНН в URL
                inn_match = re.search(r'/id/(\d{10})', url_path)
                if inn_match:
                    company_data['inn'] = inn_match.group(1)
            
            if not company_data['inn']:
                logger.debug(f"[{self.source_name}] No INN found: {company_data['name']}")
                return None
            
            # 3. Извлечение выручки (rusprofile показывает выручку в финансовых показателях)
            revenue = self._extract_revenue_rusprofile(soup, text_lower)
            if not revenue or revenue < 200000000:
                logger.debug(f"[{self.source_name}] Revenue < 200M: {company_data['name']}")
                return None
            
            company_data['revenue'] = revenue
            
            # 4. Извлечение ОКВЭД
            okved = self._extract_okved_rusprofile(soup, text)
            if okved:
                company_data['okved_main'] = okved
            
            # 5. Извлечение региона
            region = self._extract_region_rusprofile(soup)
            if region:
                company_data['region'] = region
            
            # 6. Извлечение сайта
            site = self._extract_site_rusprofile(soup)
            if site:
                company_data['site'] = site
            
            # 7. Извлечение описания
            description = self._extract_description_rusprofile(soup)
            if description:
                company_data['description'] = description
            
            # 8. Определение сегмента
            company_data['segment_tag'] = self._determine_segment(company_data)
            
            # 9. Проверка релевантности
            if not self._is_relevant_profile(company_data):
                logger.debug(f"[{self.source_name}] Not relevant: {company_data['name']}")
                return None
            
            logger.info(f"[{self.source_name}] ✓ {company_data['name']} - {revenue:,} руб")
            return company_data
            
        except Exception as e:
            logger.error(f"[{self.source_name}] Parse error: {e}")
            return None
    
    def _extract_revenue_rusprofile(self, soup: BeautifulSoup, text_lower: str) -> Optional[int]:
        """Извлечение выручки с rusprofile"""
        # Ищем в финансовых показателях
        finance_section = soup.find('div', class_=re.compile(r'finance|revenue|выручка', re.I))
        if finance_section:
            finance_text = finance_section.get_text().lower()
            patterns = [
                r'выручка[^\d]{0,20}(\d[\d\s]*)\s*руб',
                r'доход[^\d]{0,20}(\d[\d\s]*)\s*руб',
                r'общая выручка[^\d]{0,20}(\d[\d\s]*)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, finance_text)
                if match:
                    revenue_str = match.group(1).replace(' ', '').replace(',', '').strip()
                    if revenue_str.isdigit():
                        revenue = int(revenue_str)
                        if revenue >= 200000000:
                            return revenue
        
        # Ищем в общем тексте
        patterns = [
            r'выручка за.*?(\d[\d\s]*)\s*руб',
            r'выручка составила.*?(\d[\d\s]*)\s*руб',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                revenue_str = match.group(1).replace(' ', '').replace(',', '').strip()
                if revenue_str.isdigit():
                    revenue = int(revenue_str)
                    if revenue >= 200000000:
                        return revenue
        
        return None
    
    def _extract_okved_rusprofile(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        """Извлечение ОКВЭД с rusprofile"""
        # Ищем в разделе "Основной вид деятельности"
        okved_section = soup.find('div', class_=re.compile(r'okved|activity|вид деятельности', re.I))
        if okved_section:
            okved_text = okved_section.get_text()
            match = re.search(r'(\d{2}\.\d{2}(?:\.\d{1,2})?)', okved_text)
            if match:
                return match.group(1)
        
        # Ищем в общем тексте
        match = re.search(r'ОКВЭД[^:\d]*[:]?\s*([\d]{2}[\.][\d]{2})', text, re.IGNORECASE)
        if match:
            return match.group(1)
        
        return None
    
    def _extract_region_rusprofile(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение региона с rusprofile"""
        # Ищем адрес компании
        address_section = soup.find('div', class_=re.compile(r'address|адрес|location', re.I))
        if address_section:
            address_text = address_section.get_text(strip=True)
            # Извлекаем город/регион из адреса
            city_match = re.search(r'(г\.|город|москва|санкт-петербург|спб)[^\d,]{0,20}[,\s]', 
                                  address_text, re.IGNORECASE)
            if city_match:
                return city_match.group(0).strip(' ,')
        
        return None
    
    def _extract_site_rusprofile(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение сайта с rusprofile"""
        # Ищем ссылку на сайт
        site_link = soup.find('a', class_=re.compile(r'site|website|сайт', re.I))
        if site_link and site_link.get('href'):
            href = site_link.get('href')
            if href.startswith('http'):
                return href
        
        return None
    
    def _extract_description_rusprofile(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение описания с rusprofile"""
        # Ищем описание деятельности
        desc_section = soup.find('div', class_=re.compile(r'description|about|описание', re.I))
        if desc_section:
            desc_text = desc_section.get_text(strip=True)
            if len(desc_text) > 20 and len(desc_text) < 300:
                # Проверяем, что это не юридическая информация
                legal_keywords = ['инн', 'огрн', 'окпо', 'ооо', 'зао', 'ао']
                if not any(keyword in desc_text.lower() for keyword in legal_keywords):
                    return desc_text
        
        return None
    
    def _determine_segment(self, company_data: Dict) -> str:
        """Определение сегмента компании"""
        name = str(company_data.get('name', '')).lower()
        desc = str(company_data.get('description', '')).lower()
        okved = str(company_data.get('okved_main', ''))
        
        text = f"{name} {desc}"
        segments = []
        
        # BTL
        btl_keywords = ['btl', 'промо', 'ивент', 'event', 'мерчандайзинг', 'бренд-актив']
        if any(k in text for k in btl_keywords) or okved.startswith('73.11'):
            segments.append('BTL')
        
        # Сувенир
        souvenir_keywords = ['сувенир', 'подар', 'полиграф', 'печат', 'брендирован']
        if any(k in text for k in souvenir_keywords) or okved.startswith('18.12'):
            segments.append('SOUVENIR')
        
        # Полный цикл
        full_cycle_keywords = ['полный цикл', 'full cycle', 'комплексн', 'интегрирован']
        if any(k in text for k in full_cycle_keywords):
            segments.append('FULL_CYCLE')
        
        # Коммуникационные группы
        comm_keywords = ['коммуникац', 'комм груп', 'агентств', 'рекламн', 'маркетингов', 'pr']
        if any(k in text for k in comm_keywords) or okved.startswith('73.12'):
            segments.append('COMM_GROUP')
        
        return '|'.join(segments) if segments else 'OTHER'
    
    def collect_companies(self, search_queries: List[str], max_companies: int = 100) -> List[Dict]:
        """Сбор компаний с rusprofile"""
        all_companies = []
        processed_inns = set()
        
        logger.info(f"[{self.source_name}] Starting collection (max {max_companies})")
        
        for query in search_queries:
            if len(all_companies) >= max_companies:
                break
            
            logger.info(f"[{self.source_name}] Searching: '{query}'")
            company_links = self.search_companies(query, pages=2)
            
            if not company_links:
                continue
            
            random.shuffle(company_links)
            
            for link in company_links:
                if len(all_companies) >= max_companies:
                    break
                
                company_data = self.parse_company_page(link)
                
                if company_data:
                    inn = company_data['inn']
                    if inn not in processed_inns:
                        processed_inns.add(inn)
                        all_companies.append(company_data)
                
                time.sleep(random.uniform(2, 4))
        
        logger.info(f"[{self.source_name}] Collection complete: {len(all_companies)} companies")
        return all_companies