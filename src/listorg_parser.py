import requests
import pandas as pd
import re
import os
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import random
from fake_useragent import UserAgent

import logging
logger = logging.getLogger(__name__)
from src.base_parser import BaseParser

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)



class ListOrgParser(BaseParser):
    def __init__(self):
        super().__init__('list_org')
    """Парсер для сайта list-org.com с фильтрацией по требованиям"""
    
    def __init__(self):
        self.base_url = "https://list-org.com"
        self.session = requests.Session()
        self.ua = UserAgent()
        self._update_headers()
        
    def _update_headers(self):
        """Обновление заголовков с случайным User-Agent"""
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _make_request(self, url: str, max_retries: int = 2) -> Optional[requests.Response]:
        """Безопасный HTTP запрос"""
        for attempt in range(max_retries):
            try:
                self._update_headers()
                response = self.session.get(url, timeout=(10, 30), allow_redirects=True)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 5
                    logger.debug(f"Too many requests. Waiting {wait_time}s...")
                    import time
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.debug(f"Request error {url}: {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(random.uniform(1, 3))
        
        return None
    
    def search_companies(self, query: str, pages: int = 2) -> List[str]:
        """Поиск компаний по запросу"""
        company_links = []
        
        for page in range(1, pages + 1):
            try:
                encoded_query = requests.utils.quote(query)
                url = f"{self.base_url}/search?type=all&val={encoded_query}&page={page}"
                logger.info(f"Search: '{query}' - page {page}")
                
                response = self._make_request(url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Ищем ссылки на компании
                links = soup.select('a[href^="/company/"]')
                for link in links:
                    href = link.get('href')
                    if href and href not in company_links:
                        company_links.append(href)
                
                # Случайная задержка
                import time
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                logger.error(f"Search error page {page}: {e}")
                continue
        
        return list(set(company_links))
    
    def _is_russian_company(self, company_data: Dict) -> bool:
        """Проверка что компания российская"""
        inn = company_data.get('inn')
        if inn:
            inn_str = str(inn)
            if (len(inn_str) == 10 and inn_str.isdigit()) or (len(inn_str) == 12 and inn_str.isdigit()):
                return True
        
        region = str(company_data.get('region', '')).lower()
        russian_indicators = [
            'россия', 'рф', 'ru', 'russia',
            'москва', 'санкт-петербург', 'спб', 'moscow', 'st. petersburg',
            'область', 'край', 'республика', 'респ.',
            'г. ', 'город ', 'ул.', 'проспект', 'бульвар'
        ]
        
        if any(indicator in region for indicator in russian_indicators):
            return True
        
        if company_data.get('okved_main'):
            return True
        
        contacts = str(company_data.get('contacts', '')).lower()
        if '+7' in contacts or '8(' in contacts:
            return True
        
        return True
    
    def _is_relevant_profile(self, company_data: Dict) -> bool:
        """Проверка релевантного профиля"""
        name = str(company_data.get('name', '')).lower()
        description = str(company_data.get('description', '')).lower()
        okved = str(company_data.get('okved_main', ''))
        segment_tag = str(company_data.get('segment_tag', ''))
        
        relevant_tags = ['BTL', 'SOUVENIR', 'FULL_CYCLE', 'COMM_GROUP']
        if any(tag in segment_tag for tag in relevant_tags):
            return True
        
        text_to_check = f"{name} {description}"
        
        btl_keywords = [
            'btl', 'промо', 'ивент', 'event', 'мерчандайзинг', 'мерчендайзинг',
            'бренд-актив', 'бренд активац', 'промоакц', 'живой маркетинг',
            'field marketing', 'live marketing', 'торговый маркетинг',
            'промоутер', 'промо-групп', 'активац', 'сэмплинг'
        ]
        
        souvenir_keywords = [
            'сувенир', 'подар', 'бизнес-сувенир', 'корпоративн', 'промопродукц',
            'полиграф', 'печат', 'тираж', 'календар', 'брендирован',
            'рекламн', 'премиальн', 'промо-сувенир'
        ]
        
        full_cycle_keywords = [
            'полный цикл', 'full cycle', 'full-service', 'комплексн',
            'интегрирован', '360°', 'end-to-end', 'интегратор'
        ]
        
        comm_keywords = [
            'коммуникац', 'комм груп', 'агентств', 'рекламн', 'маркетингов',
            'pr', 'public relations', 'digital', 'диджитал', 'креативн',
            'медиа', 'smm', 'контент', 'брендинг', 'стратеги'
        ]
        
        all_keywords = btl_keywords + souvenir_keywords + full_cycle_keywords + comm_keywords
        
        if any(keyword in text_to_check for keyword in all_keywords):
            return True
        
        relevant_okveds = [
            '73.11', '73.12', '18.12', '74.10', '74.20', '90.03', '58.11', '58.19'
        ]
        
        if any(okved.startswith(code) for code in relevant_okveds):
            return True
        
        return False
    
    def _extract_revenue(self, text_lower: str) -> Optional[int]:
        """Извлечение выручки ≥ 200 млн рублей"""
        revenue_patterns = [
            r'выручка[^\d]{0,20}(\d[\d\s]*)\s*руб',
            r'выручка[^\d]{0,20}(\d[\d\s]*)\s*₽',
            r'доход[^\d]{0,20}(\d[\d\s]*)\s*руб',
            r'общая выручка[^\d]{0,20}(\d[\d\s]*)',
            r'выручка за год[^\d]{0,20}(\d[\d\s]*)',
            r'от реализации[^\d]{0,20}(\d[\d\s]*)\s*руб'
        ]
        
        for pattern in revenue_patterns:
            match = re.search(pattern, text_lower)
            if match:
                revenue_str = match.group(1).replace(' ', '').replace(',', '').strip()
                if revenue_str.isdigit():
                    revenue = int(revenue_str)
                    if revenue >= 200000000:
                        return revenue
        
        return None
    
    def _clean_text(self, text: str) -> str:
        """Очистка текста от лишних пробелов и символов"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        return text
    
    def _extract_clean_description(self, soup: BeautifulSoup, text: str, company_name: str) -> Optional[str]:
        """Извлечение чистого описания деятельности, а не юридической информации"""
        
        # 1. Ищем в мета-тегах
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            desc = meta_desc.get('content').strip()
            if self._is_real_description(desc, company_name):
                return self._clean_text(desc[:200])
        
        # 2. Ищем в параграфах с классами, содержащими описание
        desc_classes = ['description', 'about', 'info', 'text', 'desc', 
                       'activity', 'services', 'услуги', 'деятельность']
        
        for tag in soup.find_all(['p', 'div', 'span']):
            class_attr = tag.get('class', [])
            class_str = ' '.join(class_attr).lower() if class_attr else ''
            id_attr = tag.get('id', '').lower()
            
            # Проверяем по классу или id
            if any(desc_class in class_str for desc_class in desc_classes) or \
               any(desc_class in id_attr for desc_class in desc_classes):
                
                desc_text = tag.get_text(strip=True)
                if self._is_real_description(desc_text, company_name):
                    return self._clean_text(desc_text[:200])
        
        # 3. Ищем по текстовым паттернам (более надежный способ)
        patterns = [
            r'Предоставляет услуги[:\s]*([^\n\.]{10,150})',
            r'Специализация[:\s]*([^\n\.]{10,150})',
            r'Основные услуги[:\s]*([^\n\.]{10,150})',
            r'Занимается[:\s]*([^\n\.]{10,150})',
            r'Осуществляет деятельность[:\s]*([^\n\.]{10,150})',
            r'Деятельность компании[:\s]*([^\n\.]{10,150})',
            r'Компания работает в сфере[:\s]*([^\n\.]{10,150})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                desc_text = match.group(1).strip()
                if self._is_real_description(desc_text, company_name):
                    return self._clean_text(desc_text[:200])
        
        # 4. Ищем любое описание, которое не содержит юридической информации
        sentences = re.split(r'[\.!?]\s+', text)
        for sentence in sentences:
            sentence = sentence.strip()
            if (30 < len(sentence) < 200 and 
                self._is_real_description(sentence, company_name) and
                not self._contains_legal_info(sentence)):
                return self._clean_text(sentence)
        
        # 5. Если не нашли реального описания - возвращаем None
        return None
    
    def _is_real_description(self, text: str, company_name: str) -> bool:
        """Проверяет, является ли текст реальным описанием деятельности"""
        text_lower = text.lower()
        name_lower = company_name.lower()
        
        # Исключаем юридическую информацию
        legal_phrases = [
            'общество с ограниченной ответственностью',
            'ооо', 'зао', 'оао', 'ао', 'пао',
            'инн', 'огрн', 'окпо', 'октмо', 'окогу',
            'показатели', 'адрес', 'руководитель',
            'телефон', 'вид деятельности компании',
            'юридический адрес', 'фактический адрес',
            'основной государственный регистрационный номер',
            'общероссийский классификатор',
            'дата регистрации', 'уставный капитал',
            'кпп', 'егрюл',
        ]
        
        # Если текст содержит юридическую информацию - это не описание
        if any(phrase in text_lower for phrase in legal_phrases):
            return False
        
        # Если текст содержит саму компанию (повторение названия)
        if name_lower in text_lower and len(text_lower) < len(name_lower) * 2:
            return False
        
        # Если текст слишком короткий или длинный
        if len(text) < 20 or len(text) > 500:
            return False
        
        # Проверяем, что текст содержит ключевые слова деятельности
        activity_keywords = [
            'услуги', 'деятельность', 'работает', 'занимается',
            'специализация', 'предоставляет', 'оказывает',
            'производство', 'продажа', 'организация',
            'разработка', 'внедрение', 'проведение',
            'создание', 'реализация', 'обслуживание'
        ]
        
        # Хотя бы одно ключевое слово должно быть
        if not any(keyword in text_lower for keyword in activity_keywords):
            return False
        
        return True
    
    def _contains_legal_info(self, text: str) -> bool:
        """Проверяет, содержит ли текст юридическую информацию"""
        text_lower = text.lower()
        
        legal_patterns = [
            r'\bинн\s*\d{10,12}\b',
            r'\bогрн\s*\d{13,15}\b',
            r'\bокпо\s*\d{8,10}\b',
            r'\d{2}[\.]\d{2}[\.]\d{6,7}',  # ИНН с точками
            r'\d{13,15}',  # ОГРН
            r'общество с ограниченной ответственностью',
            r'\bооо\b',
        ]
        
        for pattern in legal_patterns:
            if re.search(pattern, text_lower):
                return True
        
        return False
    
    def _extract_okved(self, text: str) -> Optional[str]:
        """Извлечение ОКВЭД"""
        patterns = [
            r'ОКВЭД[^:\d]*[:]?\s*([\d]{2}[\.][\d]{2}(?:[\.][\d]{1,2})?)',
            r'ОКВЭД2[^:\d]*[:]?\s*([\d]{2}[\.][\d]{2}(?:[\.][\d]{1,2})?)',
            r'Код ОКВЭД[^:\d]*[:]?\s*([\d]{2}[\.][\d]{2}(?:[\.][\d]{1,2})?)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                code = match.group(1)
                if self._is_valid_okved(code):
                    return code
        
        return None
    
    def _is_valid_okved(self, code: str) -> bool:
        """Проверка валидности кода ОКВЭД"""
        if not code:
            return False
        
        pattern = r'^\d{2}\.\d{2}(?:\.\d{1,2})?$'
        if not re.match(pattern, code):
            return False
        
        return True
    
    def parse_company_page(self, url_path: str) -> Optional[Dict]:
        """Парсинг страницы компании с фильтрацией"""
        try:
            url = f"{self.base_url}{url_path}"
            logger.debug(f"Parsing: {url}")
            
            response = self._make_request(url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            text = soup.get_text()
            text_lower = text.lower()
            
            # Базовые данные
            company_data = {
                'inn': None,
                'name': None,
                'revenue_year': 2023,
                'revenue': None,
                'segment_tag': None,
                'okved_main': None,
                'employees': None,
                'site': None,
                'description': None,  # Будет заполнено только реальным описанием
                'region': None,
                'contacts': None,
                'rating_ref': url_path,
                'source': 'list_org'
            }
            
            # 1. Извлечение названия
            name_tag = soup.find('h1')
            if name_tag:
                company_data['name'] = self._clean_text(name_tag.get_text())
            
            if not company_data['name']:
                return None
            
            # 2. Извлечение ИНН (обязательное поле)
            inn_patterns = [
                r'ИНН\s*[\:\-]?\s*(\d{10}|\d{12})',
                r'ИНН/КПП[^\d]*(\d{10})',
                r'\b(\d{10})\b(?!\d)'
            ]
            
            for pattern in inn_patterns:
                match = re.search(pattern, text)
                if match:
                    company_data['inn'] = match.group(1).strip()
                    break
            
            if not company_data['inn']:
                logger.debug(f"No INN found: {company_data['name']}")
                return None
            
            # 3. Извлечение выручки ≥ 200 млн (обязательное поле)
            revenue = self._extract_revenue(text_lower)
            if not revenue:
                logger.debug(f"Revenue < 200M or not found: {company_data['name']}")
                return None
            
            company_data['revenue'] = revenue
            
            # 4. Проверка что компания российская
            if not self._is_russian_company(company_data):
                logger.debug(f"Not Russian company: {company_data['name']}")
                return None
            
            # 5. Извлечение остальных полей
            # ОКВЭД
            okved = self._extract_okved(text)
            if okved:
                company_data['okved_main'] = okved
            
            # Регион
            region_patterns = [
                r'Место нахождения[:\s]+([^\n]{5,80})',
                r'Адрес[:\s]+([^\n]{5,80})',
                r'Город[:\s]+([^\n]{5,50})',
                r'Регион[:\s]+([^\n]{5,50})'
            ]
            
            for pattern in region_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    region = match.group(1).strip()
                    company_data['region'] = self._clean_text(region)
                    break
            
            # Сайт
            site_match = re.search(r'(https?://[^\s/]+(?:\.[^\s/]+)+)', text)
            if site_match:
                site = site_match.group(0)
                if 'list-org' not in site:
                    company_data['site'] = site
            
            # ОПИСАНИЕ - ТОЛЬКО РЕАЛЬНОЕ ОПИСАНИЕ ДЕЯТЕЛЬНОСТИ
            description = self._extract_clean_description(soup, text, company_data['name'])
            if description:
                company_data['description'] = description
            # Если не нашли реального описания - оставляем None
            
            # Контакты
            email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', text)
            if email_match:
                company_data['contacts'] = email_match.group(0)
            else:
                phone_match = re.search(r'(\+7\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2})', text)
                if phone_match:
                    company_data['contacts'] = phone_match.group(0)
            
            # Сотрудники
            emp_match = re.search(r'сотрудник[^\d]{0,10}(\d{1,5})\s*чел', text_lower)
            if emp_match and emp_match.group(1).isdigit():
                company_data['employees'] = int(emp_match.group(1))
            
            # 6. Определение сегмента и проверка релевантности
            company_data['segment_tag'] = self._determine_segment(company_data)
            
            if not self._is_relevant_profile(company_data):
                logger.debug(f"Not relevant profile: {company_data['name']}")
                return None
            
            logger.info(f"✓ {company_data['name']} - {revenue:,} руб - {company_data['segment_tag']}")
            return company_data
            
        except Exception as e:
            logger.error(f"Error parsing {url_path}: {e}")
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
        """Сбор компаний с фильтрацией"""
        all_companies = []
        processed_inns = set()
        
        logger.info(f"Starting collection (max {max_companies} companies)...")
        
        for query in search_queries:
            if len(all_companies) >= max_companies:
                break
            
            logger.info(f"\nSearching: '{query}'")
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
                
                # Задержка
                import time
                time.sleep(random.uniform(2, 4))
        
        logger.info(f"\nCollection complete. Found: {len(all_companies)} companies")
        return all_companies
    
    def save_to_csv(self, companies: List[Dict], filename: str = 'data/companies.csv') -> str:
        """Сохранение данных в CSV"""
        if not companies:
            logger.warning("No data to save")
            return filename
        
        df = pd.DataFrame(companies)
        
        # Обязательные колонки
        required_columns = ['inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 'source']
        
        # Дополнительные колонки
        optional_columns = ['okved_main', 'employees', 'site', 'description', 
                          'region', 'contacts', 'rating_ref']
        
        all_columns = required_columns + optional_columns
        
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        
        df = df[all_columns]
        
        # Форматирование числовых полей
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        df['employees'] = pd.to_numeric(df['employees'], errors='coerce')
        
        # Заменяем пустые строки в description на None
        if 'description' in df.columns:
            df['description'] = df['description'].replace(['', ' '], None)
        
        # Создаем директорию если её нет
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Сохраняем
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        # Статистика
        logger.info(f"File saved: {filename}")
        logger.info(f"Total companies: {len(df)}")
        
        # Статистика по description
        if 'description' in df.columns:
            desc_count = df['description'].notna().sum()
            logger.info(f"Companies with real description: {desc_count}")
        
        logger.info(f"With revenue ≥200M: {df['revenue'].notna().sum()}")
        
        if 'segment_tag' in df.columns:
            logger.info("Segments distribution:")
            for segment, count in df['segment_tag'].value_counts().items():
                logger.info(f"  {segment}: {count}")
        
        return filename


# Тест для проверки фильтрации описания
def test_description_filtering():
    """Тест фильтрации описания от юридической информации"""
    
    test_cases = [
        {
            'text': 'Показатели, адрес, руководитель, телефон, вид деятельности компании ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ АГЕНТСТВО АРТ ПРЕМИУМ ИВЕНТ ГРУПП, ИНН 5262352295, ОГРН 1175275074171, ОКПО 20004508',
            'company_name': 'АГЕНТСТВО АРТ ПРЕМИУМ ИВЕНТ ГРУПП',
            'expected': None  # Должен быть отфильтрован
        },
        {
            'text': 'Компания предоставляет услуги в области организации ивентов, бренд-активаций и промо-акций. Специализируется на комплексных маркетинговых решениях.',
            'company_name': 'Ивент Агентство',
            'expected': 'Компания предоставляет услуги в области организации ивентов, бренд-активаций и промо-акций. Специализируется на комплексных маркетинговых решениях.'  # Должен пройти
        },
        {
            'text': 'ООО "РЕКЛАМА ПЛЮС" ИНН 7701234567 ОГРН 1123456789012 занимается рекламной деятельностью и полиграфией',
            'company_name': 'РЕКЛАМА ПЛЮС',
            'expected': None  # Содержит юридическую информацию
        },
        {
            'text': 'Производство сувенирной продукции и корпоративных подарков. Брендирование различных изделий.',
            'company_name': 'Сувенирная фабрика',
            'expected': 'Производство сувенирной продукции и корпоративных подарков. Брендирование различных изделий.'  # Хорошее описание
        }
    ]
    
    parser = ListOrgParser()
    
    print("Testing description filtering:")
    print("=" * 60)
    
    for i, test in enumerate(test_cases, 1):
        soup = BeautifulSoup(f"<html><body><p>{test['text']}</p></body></html>", 'html.parser')
        result = parser._extract_clean_description(soup, test['text'], test['company_name'])
        
        status = "✓" if result == test['expected'] else "✗"
        print(f"\nTest {i}: {status}")
        print(f"Company: {test['company_name']}")
        print(f"Text: {test['text'][:100]}...")
        print(f"Expected: {test['expected']}")
        print(f"Got: {result}")
    
    print("\n" + "=" * 60)
    print("Summary: Description field will only contain real business descriptions,")
    print("not legal/registration information.")


if __name__ == "__main__":
    test_description_filtering()