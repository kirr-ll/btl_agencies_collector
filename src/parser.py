import requests
import pandas as pd
import time
import re
import os
from bs4 import BeautifulSoup
import logging
from typing import List, Dict, Optional
import random
from fake_useragent import UserAgent  # Добавлен новый импорт

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ListOrgParser:
    """Парсер для сайта list-org.com"""
    
    def __init__(self):
        self.base_url = "https://list-org.com"
        self.session = requests.Session()
        self.ua = UserAgent()  # Генератор случайных User-Agent
        self._update_headers()
        self.companies = []
        
    def _update_headers(self):
        """Обновление заголовков с случайным User-Agent"""
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        })
    
    def _make_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Безопасный HTTP запрос с повторными попытками"""
        for attempt in range(max_retries):
            try:
                # Обновляем User-Agent для каждого запроса
                self._update_headers()
                
                response = self.session.get(
                    url, 
                    timeout=(10, 30),  # 10 секунд на подключение, 30 на чтение
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Too Many Requests
                    wait_time = (attempt + 1) * 10  # Увеличиваем задержку
                    logger.warning(f"Слишком много запросов. Ждем {wait_time} сек...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"HTTP {response.status_code} для {url}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Таймаут запроса {url}, попытка {attempt + 1}")
            except requests.exceptions.ConnectionError:
                logger.warning(f"Ошибка подключения {url}, попытка {attempt + 1}")
            except Exception as e:
                logger.warning(f"Ошибка запроса {url}: {e}")
            
            # Задержка перед повторной попыткой
            if attempt < max_retries - 1:
                time.sleep(random.uniform(2, 5))
        
        return None
    
    def search_companies(self, query: str, pages: int = 3) -> List[str]:
        """Поиск компаний по запросу"""
        company_links = []
        
        for page in range(1, pages + 1):
            try:
                encoded_query = requests.utils.quote(query)
                url = f"{self.base_url}/search?type=all&val={encoded_query}&page={page}"
                logger.info(f"Поиск: '{query}' - страница {page}")
                
                response = self._make_request(url)
                if not response:
                    continue
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Ищем ссылки на компании в таблице
                company_rows = soup.select('table.list>tr')
                for row in company_rows:
                    link_tag = row.select_one('td:nth-child(2) a[href^="/company/"]')
                    if link_tag:
                        href = link_tag.get('href')
                        if href and href.startswith('/company/') and href not in company_links:
                            company_links.append(href)
                
                # Альтернативный поиск ссылок
                if not company_rows:
                    company_links_fallback = soup.select('a[href^="/company/"]')
                    for link in company_links_fallback:
                        href = link.get('href')
                        if href and href not in company_links:
                            company_links.append(href)
                
                # Если нет компаний на странице, прекращаем
                if not company_rows and not company_links_fallback:
                    logger.debug(f"На странице {page} не найдено компаний")
                    break
                    
                # Случайная задержка
                time.sleep(random.uniform(2, 4))
                
            except Exception as e:
                logger.error(f"Ошибка поиска страницы {page}: {e}")
                continue
        
        logger.info(f"По запросу '{query}' найдено {len(set(company_links))} уникальных компаний")
        return list(set(company_links))
    
    def parse_company_page(self, url_path: str) -> Optional[Dict]:
        """Парсинг страницы компании"""
        try:
            url = f"{self.base_url}{url_path}"
            logger.debug(f"Парсинг компании: {url}")
            
            response = self._make_request(url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            company_data = {
                'inn': None,
                'name': None,
                'revenue_year': 2023,  # Последний доступный год
                'revenue': None,
                'segment_tag': None,
                'okved_main': None,
                'employees': None,
                'site': None,
                'description': None,
                'region': None,
                'contacts': None,
                'rating_ref': url_path,  # Ссылка на страницу компании
                'source': 'list_org'
            }
            
            # Извлечение названия компании
            name_tag = soup.find('h1')
            if name_tag:
                company_data['name'] = name_tag.get_text(strip=True)
            else:
                # Альтернативный поиск названия
                name_tag = soup.find('div', class_=re.compile(r'name|title', re.I))
                if name_tag:
                    company_data['name'] = name_tag.get_text(strip=True)
            
            # Если нет названия, пропускаем компанию
            if not company_data['name']:
                logger.warning(f"Не найдено название компании: {url}")
                return None
            
            # Получаем весь текст страницы
            text = soup.get_text()
            text_lower = text.lower()
            
            # Извлечение ИНН
            inn_pattern = r'ИНН\s*[\:\-]?\s*(\d{10}|\d{12})'
            inn_match = re.search(inn_pattern, text, re.IGNORECASE)
            if inn_match:
                company_data['inn'] = inn_match.group(1)
            else:
                # Дополнительный поиск ИНН
                inn_pattern2 = r'\b\d{10}\b|\b\d{12}\b'
                all_numbers = re.findall(inn_pattern2, text)
                for num in all_numbers:
                    if len(num) in [10, 12]:
                        # Простая валидация ИНН
                        if (len(num) == 10 and num.isdigit()) or (len(num) == 12 and num.isdigit()):
                            company_data['inn'] = num
                            break
            
            # Извлечение выручки
            revenue = self._extract_revenue(text_lower)
            if revenue and revenue >= 200000000:  # 200 млн руб
                company_data['revenue'] = revenue
            else:
                # Если выручка меньше 200 млн, компания нам не подходит
                logger.debug(f"Выручка меньше 200 млн или не найдена: {company_data['name']}")
                return None
            
            # Извлечение ОКВЭД
            okved = self._extract_okved(text)
            if okved:
                company_data['okved_main'] = okved
            
            # Извлечение региона
            region = self._extract_region(soup, text)
            if region:
                company_data['region'] = region
            
            # Извлечение сайта
            site = self._extract_website(soup, text)
            if site:
                company_data['site'] = site
            
            # Извлечение описания
            description = self._extract_description(soup)
            if description:
                company_data['description'] = description
            
            # Извлечение контактов
            contacts = self._extract_contacts(text)
            if contacts:
                company_data['contacts'] = contacts
            
            # Извлечение количества сотрудников
            employees = self._extract_employees(text_lower)
            if employees:
                company_data['employees'] = employees
            
            # Определение сегмента
            segment_tags = self._determine_segment(company_data)
            company_data['segment_tag'] = '|'.join(segment_tags) if segment_tags else 'OTHER'
            
            # Проверка минимальных требований
            if company_data.get('inn') and company_data.get('revenue'):
                logger.info(f"✓ {company_data['name']} - {company_data.get('revenue', 0):,} руб")
                return company_data
            else:
                logger.debug(f"✗ {company_data['name']} - нет ИНН или выручки")
                return None
            
        except Exception as e:
            logger.error(f"Ошибка парсинга компании {url_path}: {e}")
            return None
    
    def _extract_revenue(self, text_lower: str) -> Optional[int]:
        """Извлечение выручки из текста"""
        revenue_patterns = [
            r'выручка[^\d]{0,20}(\d[\d\s]*)\s*руб',
            r'выручка[^\d]{0,20}(\d[\d\s]*)\s*₽',
            r'прибыль[^\d]{0,20}(\d[\d\s]*)\s*руб',
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
                    # Проверяем масштаб (от 1 млн до 1 трлн руб)
                    if 1000000 <= revenue <= 1000000000000:
                        return revenue
        
        return None
    
    def _extract_okved(self, text: str) -> Optional[str]:
        """Извлечение ОКВЭД"""
        okved_patterns = [
            r'ОКВЭД[^\.\d]*([\d\.]{2,})',
            r'Основной вид деятельности[^\.\d]*([\d\.]{2,})',
            r'Код ОКВЭД[^\.\d]*([\d\.]{2,})',
            r'ОКВЭД2[^\.\d]*([\d\.]{2,})',
            r'Вид деятельности[^\.\d]*([\d\.]{2,})'
        ]
        
        for pattern in okved_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                okved = match.group(1)
                # Проверяем формат ОКВЭД (например, 73.11 или 73.11.1)
                if re.match(r'\d{2}\.\d{2}(?:\.\d{1,2})?', okved):
                    return okved
        
        return None
    
    def _extract_region(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        """Извлечение региона"""
        # Поиск в таблице информации
        info_table = soup.find('table', class_=re.compile(r'info|details', re.I))
        if info_table:
            rows = info_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    if any(keyword in label for keyword in ['адрес', 'место', 'регион', 'город', 'location']):
                        region = cells[1].get_text(strip=True)
                        if region and len(region) < 100:
                            return region
        
        # Поиск по шаблонам в тексте
        region_patterns = [
            r'Место нахождения[:\s]+([^\n]{5,80})',
            r'Адрес[:\s]+([^\n]{5,80})',
            r'Город[:\s]+([^\n]{5,50})',
            r'Регион[:\s]+([^\n]{5,50})',
            r'Юридический адрес[:\s]+([^\n]{10,150})',
            r'Адрес организации[:\s]+([^\n]{10,150})'
        ]
        
        for pattern in region_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                region = match.group(1).strip()
                # Очищаем от лишней информации
                region = re.sub(r'ИНН.*|\d{6},?|тел.*|факс.*', '', region)
                region = region.strip(' ,;:')
                if 3 < len(region) < 100:
                    return region
        
        return None
    
    def _extract_website(self, soup: BeautifulSoup, text: str) -> Optional[str]:
        """Извлечение сайта"""
        # Поиск в ссылках
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if href.startswith('http'):
                # Исключаем популярные сервисы
                excluded = ['list-org', 'google', 'yandex', 'mail.', 'vk.', 'facebook', 
                           'instagram', 'twitter', 'linkedin', 'youtube', 'whatsapp']
                if not any(x in href for x in excluded):
                    # Извлекаем домен
                    domain_match = re.search(r'(https?://[^/\s]+)', href)
                    if domain_match:
                        return domain_match.group(1)
        
        # Поиск по тексту
        site_pattern = r'(?:https?://)?(?:www\.)?[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?'
        site_matches = re.findall(site_pattern, text)
        for site in site_matches:
            site_lower = site.lower()
            if not any(x in site_lower for x in ['list-org', 'google', 'yandex']):
                if not site.startswith('http'):
                    site = 'http://' + site
                return site
        
        return None
    
    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение описания"""
        # Поиск в мета-тегах
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            desc = meta_desc.get('content').strip()
            if 20 < len(desc) < 300:
                return desc
        
        # Поиск в div с описанием
        desc_divs = soup.find_all(['div', 'p'], 
                                 class_=re.compile(r'desc|about|info|text|annotation', re.I))
        for div in desc_divs:
            text = div.get_text(strip=True)
            if 30 < len(text) < 500:
                # Проверяем, что это действительно описание
                if not any(x in text.lower() for x in ['©', 'copyright', 'все права']):
                    # Убираем лишние пробелы
                    text = re.sub(r'\s+', ' ', text)
                    return text[:300]  # Обрезаем если слишком длинное
        
        return None
    
    def _extract_contacts(self, text: str) -> Optional[str]:
        """Извлечение контактов"""
        # Поиск email
        email_pattern = r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
        emails = re.findall(email_pattern, text)
        if emails:
            return emails[0]
        
        # Поиск телефона
        phone_patterns = [
            r'\+7\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
            r'8\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
            r'тел[\.:\s]+([+\d\s\-\(\)]{8,20})',
            r'телефон[\.:\s]+([+\d\s\-\(\)]{8,20})',
            r'контактный телефон[\.:\s]+([+\d\s\-\(\)]{8,20})'
        ]
        
        for pattern in phone_patterns:
            phones = re.findall(pattern, text, re.IGNORECASE)
            for phone in phones:
                phone_clean = re.sub(r'\D', '', phone)
                if 10 <= len(phone_clean) <= 11:
                    return phone
        
        return None
    
    def _extract_employees(self, text_lower: str) -> Optional[int]:
        """Извлечение количества сотрудников"""
        employee_patterns = [
            r'сотрудник[^0-9]{0,10}(\d{1,5})\s*чел',
            r'численность[^0-9]{0,10}(\d{1,5})\s*чел',
            r'работает[^0-9]{0,10}(\d{1,5})\s*чел',
            r'персонал[^0-9]{0,10}(\d{1,5})\s*чел',
            r'штат[^0-9]{0,10}(\d{1,5})\s*сотрудник'
        ]
        
        for pattern in employee_patterns:
            match = re.search(pattern, text_lower)
            if match:
                emp_str = match.group(1)
                if emp_str.isdigit():
                    employees = int(emp_str)
                    if 1 <= employees <= 10000:  # Реалистичный диапазон
                        return employees
        
        return None
    
    def _determine_segment(self, company_data: Dict) -> List[str]:
        """Определение сегмента компании"""
        segments = []
        
        okved = str(company_data.get('okved_main', ''))
        description = str(company_data.get('description', '')).lower()
        name = str(company_data.get('name', '')).lower()
        site = str(company_data.get('site', '')).lower()
        
        # Объединяем все текстовые поля для анализа
        text_to_check = f"{name} {description} {site}"
        
        # BTL сегмент
        btl_keywords = [
            'btl', 'промо', 'ивент', 'мерчандайзинг', 'бренд-актив', 
            'бренд активац', 'event', 'промоакц', 'промо-акц', 'live marketing',
            'field marketing', 'торговый маркетинг'
        ]
        if any(keyword in text_to_check for keyword in btl_keywords):
            segments.append('BTL')
        
        # Сувенирный сегмент
        souvenir_keywords = [
            'сувенир', 'промопродукц', 'подар', 'рекламн', 'печат', 
            'полиграф', 'тираж', 'календар', 'брендирован', 'корпоративн',
            'промо-сувенир', 'бизнес-сувенир'
        ]
        if any(keyword in text_to_check for keyword in souvenir_keywords):
            segments.append('SOUVENIR')
        
        # Полный цикл
        full_cycle_keywords = [
            'полный цикл', 'full cycle', 'комплексн', 'интегрирован', 
            'full-service', 'fullservice', 'интегратор', 'комплексные решения',
            '360°', 'end-to-end'
        ]
        if any(keyword in text_to_check for keyword in full_cycle_keywords):
            segments.append('FULL_CYCLE')
        
        # Коммуникационные группы
        comm_group_keywords = [
            'коммуникаци', 'комм груп', 'агентств', 'рекламн', 'маркетингов',
            'pr', 'public relations', 'digital', 'диджитал', 'креативн',
            'медиа', 'smm', 'социальные сети', 'контент-маркетинг'
        ]
        if any(keyword in text_to_check for keyword in comm_group_keywords):
            segments.append('COMM_GROUP')
        
        # Проверка по ОКВЭД
        okved_codes = {
            '73.11': 'BTL',        # Деятельность рекламных агентств
            '73.12': 'COMM_GROUP', # Представление в средствах массовой информации
            '18.12': 'SOUVENIR',   # Прочие виды полиграфической деятельности
            '74.20': 'FULL_CYCLE', # Деятельность в области фотографии
            '90.03': 'BTL',        # Деятельность в области художественного творчества
            '74.10': 'COMM_GROUP', # Деятельность специализированная в области дизайна
            '74.30': 'BTL',        # Деятельность по письменному и устному переводу
            '58.11': 'SOUVENIR',   # Издание книг
            '58.19': 'SOUVENIR',   # Издание прочей печатной продукции
        }
        
        for code, segment in okved_codes.items():
            if okved.startswith(code):
                if segment not in segments:
                    segments.append(segment)
        
        # Если не определили сегмент, но компания рекламная
        if not segments:
            if 'реклам' in text_to_check or 'маркетинг' in text_to_check:
                segments.append('COMM_GROUP')
        
        return segments if segments else ['OTHER']
    
    def collect_companies(self, search_queries: List[str], max_companies: int = 100) -> List[Dict]:
        """Сбор компаний по нескольким запросам"""
        all_companies = []
        processed_inns = set()
        
        logger.info(f"Начинаем сбор до {max_companies} компаний...")
        
        for query_index, query in enumerate(search_queries):
            if len(all_companies) >= max_companies:
                logger.info(f"Достигнут лимит в {max_companies} компаний")
                break
                
            logger.info(f"\nЗапрос {query_index+1}/{len(search_queries)}: '{query}'")
            
            # Ищем ссылки на компании
            company_links = self.search_companies(query, pages=2)
            
            if not company_links:
                logger.warning(f"По запросу '{query}' не найдено компаний")
                continue
            
            # Перемешиваем ссылки для разнообразия
            random.shuffle(company_links)
            
            # Парсим каждую компанию
            for i, link in enumerate(company_links):
                if len(all_companies) >= max_companies:
                    break
                
                logger.debug(f"  Компания {i+1}/{len(company_links)}")
                
                company_data = self.parse_company_page(link)
                
                if company_data:
                    inn = company_data.get('inn')
                    if inn and inn not in processed_inns:
                        processed_inns.add(inn)
                        all_companies.append(company_data)
                        logger.info(f"  ✓ Добавлена: {company_data.get('name')} (ИНН: {inn})")
                    elif inn:
                        logger.debug(f"  ✗ Дубликат ИНН: {inn}")
                else:
                    logger.debug(f"  ✗ Пропущена (не соответствует критериям)")
                
                # Динамическая задержка
                delay = random.uniform(3, 7)
                time.sleep(delay)
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Сбор завершен. Всего компаний: {len(all_companies)}")
        return all_companies
    
    def save_to_csv(self, companies: List[Dict], filename: str = 'data/companies.csv') -> str:
        """Сохранение данных в CSV"""
        if not companies:
            logger.warning("Нет данных для сохранения")
            return filename
        
        df = pd.DataFrame(companies)
        
        # Гарантируем наличие всех колонок в правильном порядке
        column_order = [
            'inn', 'name', 'revenue_year', 'revenue', 'segment_tag', 'source',
            'okved_main', 'employees', 'site', 'description', 'region', 
            'contacts', 'rating_ref'
        ]
        
        # Добавляем отсутствующие колонки
        for col in column_order:
            if col not in df.columns:
                df[col] = None
        
        # Упорядочиваем колонки
        df = df[column_order]
        
        # Форматирование числовых полей
        if 'revenue' in df.columns:
            df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
        
        if 'employees' in df.columns:
            df['employees'] = pd.to_numeric(df['employees'], errors='coerce')
        
        # Создаем директорию, если её нет
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Сохраняем в CSV
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        logger.info(f"Основной файл сохранен: {filename}")
       
        # Статистика
        logger.info(f"Всего строк: {len(df)}")
        logger.info(f"С выручкой ≥200 млн: {df['revenue'].notna().sum()}")
        
        if 'segment_tag' in df.columns:
            segment_counts = df['segment_tag'].value_counts()
            for segment, count in segment_counts.items():
                logger.info(f"  {segment}: {count}")
        
        return filename


def test_parser():
    """Функция для тестирования парсера"""
    parser = ListOrgParser()
    
    print("Тестирование парсера...")
    
    # Тестовый запрос
    test_companies = parser.collect_companies(["BTL агентство Москва"], max_companies=5)
    
    if test_companies:
        print(f"\nСобрано {len(test_companies)} тестовых компаний:")
        for i, company in enumerate(test_companies, 1):
            print(f"\n{i}. {company.get('name')}")
            print(f"   ИНН: {company.get('inn')}")
            print(f"   Выручка: {company.get('revenue', 0):,} руб")
            print(f"   Сегмент: {company.get('segment_tag')}")
    else:
        print("Не удалось собрать тестовые данные")


if __name__ == "__main__":
    # Для тестирования напрямую
    test_parser()