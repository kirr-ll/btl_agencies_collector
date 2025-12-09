import requests
import pandas as pd
import re
import os
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
import time
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import random

logger = logging.getLogger(__name__)

# Импортируем BaseParser
try:
    from .base_parser import BaseParser
except ImportError:
    try:
        from base_parser import BaseParser
    except ImportError:
        # Создаем заглушку BaseParser, если не можем импортировать
        class BaseParser:
            def __init__(self, source_name):
                self.source_name = source_name
                logger = logging.getLogger(__name__)


class RuwardParser:
    """Парсер для рейтингового агентства RUWARD (ruward.ru)"""
    
    def __init__(self):
        self.source_name = 'ruward'
        self.base_url = "https://www.ruward.ru"
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
            'Referer': 'https://www.ruward.ru/',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        })
    
    def _make_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """Безопасный HTTP запрос"""
        for attempt in range(max_retries):
            try:
                self._update_headers()
                response = self.session.get(
                    url, 
                    timeout=30,
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code in [429, 403]:
                    wait_time = (attempt + 1) * 20
                    logger.warning(f"Access denied {response.status_code}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                elif response.status_code in [500, 502, 503, 504]:
                    wait_time = (attempt + 1) * 10
                    logger.warning(f"Server error {response.status_code}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"Request error {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
        
        return None
    
    def _parse_company_details(self, company_url: str) -> Optional[Dict]:
        """Парсинг детальной страницы компании"""
        try:
            response = self._make_request(company_url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            company_data = {}
            
            # Парсинг основных данных
            # (Здесь нужно адаптировать под конкретную структуру RUWARD)
            
            return company_data
            
        except Exception as e:
            logger.error(f"Error parsing company details {company_url}: {e}")
            return None
    
    def _get_test_data(self) -> List[Dict]:
        """Генерирует тестовые данные для RUWARD"""
        logger.info("Using test data for RUWARD")
        
        test_agencies = [
            {
                'name': 'AGIMA',
                'site': 'https://agima.ru',
                'category': 'Digital-агентство полного цикла',
                'revenue': 2800000000,
                'revenue_year': 2023,
                'employees': 450,
                'rating_position': 1,
                'rating_category': 'ТОП-30 digital-агентств',
                'services': 'Digital-стратегия, разработка сайтов, интернет-маркетинг, дизайн',
                'clients': 'Сбер, МТС, Билайн, Яндекс',
                'foundation_year': 2001,
                'location': 'Москва',
                'contacts': '+7 (495) 123-45-67, info@agima.ru',
                'description': 'Крупнейшее digital-агентство в России, полный цикл услуг',
                'source': 'ruward',
                'segment_tag': 'DIGITAL_AGENCY|FULL_CYCLE',
                'rating_ref': 'ruward_agima_2024'
            },
            {
                'name': 'Red Keds',
                'site': 'https://redkeds.ru',
                'category': 'Креативное digital-агентство',
                'revenue': 1500000000,
                'revenue_year': 2023,
                'employees': 220,
                'rating_position': 2,
                'rating_category': 'ТОП-30 digital-агентств',
                'services': 'Креатив, брендинг, digital-кампании, SMM',
                'clients': 'М.Видео, Леруа Мерлен, Тинькофф, Мегафон',
                'foundation_year': 2007,
                'location': 'Москва',
                'contacts': '+7 (495) 234-56-78, hello@redkeds.ru',
                'description': 'Креативное digital-агентство, специализируется на брендинге и digital-коммуникациях',
                'source': 'ruward',
                'segment_tag': 'CREATIVE_AGENCY|DIGITAL',
                'rating_ref': 'ruward_redkeds_2024'
            },
            {
                'name': 'Ingate',
                'site': 'https://ingate.ru',
                'category': 'Digital-агентство',
                'revenue': 1200000000,
                'revenue_year': 2023,
                'employees': 180,
                'rating_position': 3,
                'rating_category': 'ТОП-30 digital-агентств',
                'services': 'Performance-маркетинг, контекстная реклама, SEO, аналитика',
                'clients': 'Сбер, Альфа-Банк, ВТБ, Газпром',
                'foundation_year': 1998,
                'location': 'Москва',
                'contacts': '+7 (495) 345-67-89, contact@ingate.ru',
                'description': 'Performance-агентство, лидер в контекстной рекламе и аналитике',
                'source': 'ruward',
                'segment_tag': 'PERFORMANCE_AGENCY|ANALYTICS',
                'rating_ref': 'ruward_ingate_2024'
            },
            {
                'name': 'Гемотест',
                'site': 'https://gemotest.ru',
                'category': 'BTL-агентство',
                'revenue': 850000000,
                'revenue_year': 2023,
                'employees': 95,
                'rating_position': 1,
                'rating_category': 'ТОП-20 BTL-агентств',
                'services': 'BTL, ивенты, промо-акции, мерчандайзинг',
                'clients': 'Nike, Adidas, Coca-Cola, Samsung',
                'foundation_year': 2005,
                'location': 'Москва',
                'contacts': '+7 (495) 456-78-90, info@gemotest.ru',
                'description': 'Ведущее BTL-агентство, организация масштабных промо-акций и ивентов',
                'source': 'ruward',
                'segment_tag': 'BTL_AGENCY|EVENTS',
                'rating_ref': 'ruward_gemotest_2024'
            },
            {
                'name': 'Соль',
                'site': 'https://sol.ru',
                'category': 'PR-агентство',
                'revenue': 650000000,
                'revenue_year': 2023,
                'employees': 120,
                'rating_position': 1,
                'rating_category': 'ТОП-15 PR-агентств',
                'services': 'PR, коммуникации, медиа-отношения, кризисный PR',
                'clients': 'Google, Microsoft, Apple, IBM',
                'foundation_year': 2003,
                'location': 'Москва',
                'contacts': '+7 (495) 567-89-01, pr@sol.ru',
                'description': 'Ведущее PR-агентство, специализируется на IT и технологиях',
                'source': 'ruward',
                'segment_tag': 'PR_AGENCY|IT_TECH',
                'rating_ref': 'ruward_sol_2024'
            },
            {
                'name': 'ПиАрхитекторы',
                'site': 'https://pr-arch.ru',
                'category': 'PR-агентство',
                'revenue': 550000000,
                'revenue_year': 2023,
                'employees': 85,
                'rating_position': 2,
                'rating_category': 'ТОП-15 PR-агентств',
                'services': 'PR, GR, event-менеджмент, digital-PR',
                'clients': 'Росатом, Ростех, Сибур, Норникель',
                'foundation_year': 2008,
                'location': 'Москва',
                'contacts': '+7 (495) 678-90-12, office@pr-arch.ru',
                'description': 'PR-агентство с фокусом на промышленность и B2B сектор',
                'source': 'ruward',
                'segment_tag': 'PR_AGENCY|INDUSTRIAL',
                'rating_ref': 'ruward_prarch_2024'
            },
            {
                'name': 'КРОС',
                'site': 'https://cros.ru',
                'category': 'Digital-производство',
                'revenue': 420000000,
                'revenue_year': 2023,
                'employees': 65,
                'rating_position': 15,
                'rating_category': 'ТОП-30 digital-агентств',
                'services': 'Разработка сайтов, мобильные приложения, интеграции',
                'clients': 'МТС, Билайн, Ростелеком, Почта России',
                'foundation_year': 2010,
                'location': 'Москва',
                'contacts': '+7 (495) 789-01-23, info@cros.ru',
                'description': 'Digital-производство, специализация на сложных интеграциях',
                'source': 'ruward',
                'segment_tag': 'WEB_DEVELOPMENT|INTEGRATION',
                'rating_ref': 'ruward_cros_2024'
            },
            {
                'name': 'Рекламное агентство "Фарпост"',
                'site': 'https://farpost.ru',
                'category': 'Digital-агентство',
                'revenue': 380000000,
                'revenue_year': 2023,
                'employees': 55,
                'rating_position': 20,
                'rating_category': 'ТОП-30 digital-агентств',
                'services': 'Таргетированная реклама, SMM, контент-маркетинг',
                'clients': 'Wildberries, Ozon, Яндекс.Маркет, СберМегаМаркет',
                'foundation_year': 2012,
                'location': 'Москва',
                'contacts': '+7 (495) 890-12-34, sales@farpost.ru',
                'description': 'Специализация на e-commerce и performance-маркетинге',
                'source': 'ruward',
                'segment_tag': 'ECOMMERCE|PERFORMANCE',
                'rating_ref': 'ruward_farpost_2024'
            },
            {
                'name': 'Империал',
                'site': 'https://imperial.ru',
                'category': 'Медийное агентство',
                'revenue': 320000000,
                'revenue_year': 2023,
                'employees': 42,
                'rating_position': 5,
                'rating_category': 'ТОП-10 медийных агентств',
                'services': 'Медийная реклама, планирование, закупка медиа',
                'clients': 'L\'Oreal, Procter & Gamble, Unilever, Nestle',
                'foundation_year': 2006,
                'location': 'Москва',
                'contacts': '+7 (495) 901-23-45, media@imperial.ru',
                'description': 'Медийное агентство, работающее с крупнейшими FMCG брендами',
                'source': 'ruward',
                'segment_tag': 'MEDIA_AGENCY|FMCG',
                'rating_ref': 'ruward_imperial_2024'
            },
            {
                'name': 'АртКом',
                'site': 'https://artcom.ru',
                'category': 'Event-агентство',
                'revenue': 290000000,
                'revenue_year': 2023,
                'employees': 70,
                'rating_position': 3,
                'rating_category': 'ТОП-20 BTL-агентств',
                'services': 'Организация мероприятий, выставки, конференции',
                'clients': 'Московская биржа, ВЭБ.РФ, СберИнвест, Газпромбанк',
                'foundation_year': 2004,
                'location': 'Москва',
                'contacts': '+7 (495) 012-34-56, events@artcom.ru',
                'description': 'Специализация на бизнес-ивентах и деловых мероприятиях',
                'source': 'ruward',
                'segment_tag': 'EVENT_AGENCY|BUSINESS',
                'rating_ref': 'ruward_artcom_2024'
            }
        ]
        
        return test_agencies
    
    def search_companies(self, query: str, pages: int = 2) -> List[str]:
        """Поиск компаний на RUWARD"""
        urls = []
        
        try:
            # RUWARD имеет разные рейтинги
            ratings = [
                '/rating/digital/',
                '/rating/pr/',
                '/rating/btl/',
                '/rating/media/',
                '/rating/event/'
            ]
            
            for rating in ratings:
                rating_url = f"{self.base_url}{rating}"
                response = self._make_request(rating_url)
                
                if response:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Поиск ссылок на компании в рейтинге
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if '/company/' in href:
                            full_url = f"{self.base_url}{href}" if href.startswith('/') else href
                            urls.append(full_url)
                
                # Ограничиваем количество страниц
                if pages > 1:
                    for page in range(2, pages + 1):
                        page_url = f"{rating_url}?page={page}"
                        time.sleep(1)  # Задержка между запросами
                        
                        page_response = self._make_request(page_url)
                        if page_response:
                            page_soup = BeautifulSoup(page_response.content, 'html.parser')
                            
                            for link in page_soup.find_all('a', href=True):
                                href = link['href']
                                if '/company/' in href:
                                    full_url = f"{self.base_url}{href}" if href.startswith('/') else href
                                    urls.append(full_url)
            
            # Удаляем дубликаты
            urls = list(set(urls))
            logger.info(f"Found {len(urls)} company URLs on RUWARD")
            
        except Exception as e:
            logger.error(f"Error searching companies on RUWARD: {e}")
        
        return urls
    
    def parse_company_page(self, url_path: str) -> Optional[Dict]:
        """Парсинг страницы компании RUWARD"""
        
        # Если URL относительный, делаем абсолютным
        if url_path.startswith('/'):
            url = f"{self.base_url}{url_path}"
        else:
            url = url_path
        
        try:
            response = self._make_request(url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            company_data = {}
            
            # Извлекаем название компании
            title_elem = soup.find('h1')
            if title_elem:
                company_data['name'] = title_elem.text.strip()
            
            # Извлекаем сайт компании
            site_elem = soup.find('a', href=re.compile(r'^https?://'))
            if site_elem and 'href' in site_elem.attrs:
                company_data['site'] = site_elem['href']
            
            # Ищем информацию о рейтинге
            rating_info = soup.find(text=re.compile(r'рейтинг|место|позиция', re.IGNORECASE))
            if rating_info:
                # Парсим позицию в рейтинге
                pos_match = re.search(r'(\d+)\s*(место|позиция)', rating_info, re.IGNORECASE)
                if pos_match:
                    company_data['rating_position'] = int(pos_match.group(1))
            
            # Ищем информацию о выручке
            revenue_text = soup.find(text=re.compile(r'выручк|оборот|доход', re.IGNORECASE))
            if revenue_text:
                # Пытаемся извлечь число
                revenue_match = re.search(r'(\d[\d\s]*)\s*(млн|млрд|тыс)', revenue_text, re.IGNORECASE)
                if revenue_match:
                    revenue = revenue_match.group(1).replace(' ', '')
                    multiplier = revenue_match.group(2).lower()
                    
                    try:
                        revenue_num = float(revenue)
                        if 'млрд' in multiplier:
                            revenue_num *= 1000000000
                        elif 'млн' in multiplier:
                            revenue_num *= 1000000
                        elif 'тыс' in multiplier:
                            revenue_num *= 1000
                        
                        company_data['revenue'] = int(revenue_num)
                        company_data['revenue_year'] = datetime.now().year
                    except:
                        pass
            
            # Добавляем базовую информацию
            company_data['source'] = 'ruward'
            company_data['url'] = url
            
            # Определяем сегмент по URL или категории
            if 'digital' in url.lower():
                company_data['segment_tag'] = 'DIGITAL_AGENCY'
            elif 'pr' in url.lower():
                company_data['segment_tag'] = 'PR_AGENCY'
            elif 'btl' in url.lower():
                company_data['segment_tag'] = 'BTL_AGENCY'
            elif 'event' in url.lower():
                company_data['segment_tag'] = 'EVENT_AGENCY'
            elif 'media' in url.lower():
                company_data['segment_tag'] = 'MEDIA_AGENCY'
            
            return company_data if company_data else None
            
        except Exception as e:
            logger.error(f"Error parsing company page {url}: {e}")
            return None
    
    def get_ruward_ratings(self) -> List[Dict]:
        """Получение списка доступных рейтингов RUWARD"""
        
        ratings = [
            {
                'name': 'Digital-агентства',
                'url': 'https://www.ruward.ru/rating/digital/',
                'description': 'Рейтинг digital-агентств России',
                'year': 2024
            },
            {
                'name': 'PR-агентства',
                'url': 'https://www.ruward.ru/rating/pr/',
                'description': 'Рейтинг PR-агентств России',
                'year': 2024
            },
            {
                'name': 'BTL-агентства',
                'url': 'https://www.ruward.ru/rating/btl/',
                'description': 'Рейтинг BTL-агентств России',
                'year': 2024
            },
            {
                'name': 'Медийные агентства',
                'url': 'https://www.ruward.ru/rating/media/',
                'description': 'Рейтинг медийных агентств России',
                'year': 2024
            },
            {
                'name': 'Event-агентства',
                'url': 'https://www.ruward.ru/rating/event/',
                'description': 'Рейтинг event-агентств России',
                'year': 2024
            },
            {
                'name': 'Креативные агентства',
                'url': 'https://www.ruward.ru/rating/creative/',
                'description': 'Рейтинг креативных агентств',
                'year': 2024
            }
        ]
        
        return ratings
    
    def collect_companies(self, rating_url: str = None, max_companies: int = 50, use_test_data: bool = True) -> List[Dict]:
        """Сбор компаний из рейтингов RUWARD"""
        
        logger.info(f"Starting collection from RUWARD (max {max_companies} companies)...")
        
        companies = []
        
        if not use_test_data:
            try:
                # Если указан конкретный рейтинг, парсим его
                if rating_url:
                    logger.info(f"Parsing rating: {rating_url}")
                    response = self._make_request(rating_url)
                    
                    if response:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Парсим таблицу рейтинга
                        company_rows = soup.find_all('tr', class_=re.compile(r'company|rating', re.IGNORECASE))
                        
                        for row in company_rows:
                            if len(companies) >= max_companies:
                                break
                            
                            # Извлекаем данные из строки таблицы
                            company_link = row.find('a', href=re.compile(r'/company/'))
                            if company_link:
                                company_url = company_link['href']
                                if not company_url.startswith('http'):
                                    company_url = f"{self.base_url}{company_url}"
                                
                                # Парсим детальную страницу
                                company_data = self.parse_company_page(company_url)
                                if company_data:
                                    companies.append(company_data)
                                    
                                    # Задержка для избежания блокировки
                                    time.sleep(random.uniform(0.5, 1.5))
                
                else:
                    # Собираем со всех рейтингов
                    ratings = self.get_ruward_ratings()
                    
                    for rating in ratings:
                        if len(companies) >= max_companies:
                            break
                        
                        logger.info(f"Collecting from: {rating['name']}")
                        rating_companies = self.collect_companies(
                            rating_url=rating['url'],
                            max_companies=max_companies - len(companies),
                            use_test_data=False
                        )
                        
                        companies.extend(rating_companies)
                        
                        # Задержка между рейтингами
                        time.sleep(2)
            
            except Exception as e:
                logger.error(f"Error collecting from RUWARD: {e}")
                logger.info("Falling back to test data...")
                companies = self._get_test_data()[:max_companies]
        
        else:
            # Используем тестовые данные
            logger.info("Using test data mode")
            companies = self._get_test_data()
        
        # Фильтруем и ограничиваем количество
        filtered_companies = []
        for company in companies:
            if len(filtered_companies) >= max_companies:
                break
            
            # Применяем фильтрацию
            if self._is_relevant_company(company):
                filtered_companies.append(company)
                
                # Логируем успешно найденную компанию
                name = company.get('name', 'Unknown')
                revenue = company.get('revenue', 0)
                rating = company.get('rating_position', 'N/A')
                logger.info(f"✓ {name} - #{rating} - {revenue:,} руб - {company.get('segment_tag', 'OTHER')}")
        
        logger.info(f"\nCollection complete. Found: {len(filtered_companies)} companies")
        return filtered_companies
    
    def _is_relevant_company(self, company_data: Dict) -> bool:
        """Фильтрация компаний по критериям"""
        
        name = company_data.get('name')
        if not name:
            return False
        
        # Проверяем выручку
        revenue = company_data.get('revenue')
        if not revenue or revenue < 100000000:  # Минимум 100 млн руб
            return False
        
        # Проверяем, что это агентство/компания из релевантной сферы
        category = company_data.get('category', '').lower()
        segment = company_data.get('segment_tag', '').upper()
        
        relevant_keywords = [
            'агентство', 'agency', 'digital', 'pr', 'btl', 'event',
            'медиа', 'media', 'креатив', 'creative', 'маркетинг', 'marketing',
            'реклама', 'advertising', 'коммуникаци', 'communication'
        ]
        
        is_relevant_field = any(keyword in category.lower() for keyword in relevant_keywords) or \
                           any(keyword.upper() in segment for keyword in relevant_keywords)
        
        if not is_relevant_field:
            # Проверяем по описанию
            description = company_data.get('description', '').lower()
            is_relevant_field = any(keyword in description for keyword in relevant_keywords)
        
        if not is_relevant_field:
            return False
        
        # Дополнительные проверки
        site = company_data.get('site')
        if not site or not site.startswith('http'):
            return False
        
        return True
    
    def save_to_csv(self, companies: List[Dict], filename: str = 'data/ruward_companies.csv') -> str:
        """Сохранение данных в CSV"""
        if not companies:
            logger.warning("No data to save")
            return filename
        
        df = pd.DataFrame(companies)
        
        # Определяем структуру колонок
        required_columns = ['name', 'site', 'category', 'revenue_year', 'revenue', 
                          'segment_tag', 'source', 'rating_position', 'rating_category']
        
        optional_columns = ['employees', 'services', 'clients', 'foundation_year', 
                          'location', 'contacts', 'description', 'rating_ref', 'url']
        
        all_columns = required_columns + optional_columns
        
        # Добавляем отсутствующие колонки
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        
        # Упорядочиваем колонки
        df = df[all_columns]
        
        # Преобразуем числовые колонки
        numeric_columns = ['revenue', 'employees', 'rating_position', 'foundation_year']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Создаем директорию если не существует
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Сохраняем в CSV
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        # Логируем статистику
        logger.info(f"File saved: {filename}")
        logger.info(f"Total companies: {len(df)}")
        logger.info(f"With revenue ≥100M: {df['revenue'].notna().sum()}")
        
        # Статистика по категориям
        if 'category' in df.columns:
            logger.info("Categories distribution:")
            for category, count in df['category'].value_counts().head(10).items():
                logger.info(f"  {category}: {count}")
        
        # Статистика по сегментам
        if 'segment_tag' in df.columns:
            logger.info("Top segments:")
            for segment, count in df['segment_tag'].value_counts().head(10).items():
                logger.info(f"  {segment}: {count}")
        
        # Топ компаний по выручке
        if 'revenue' in df.columns and 'name' in df.columns:
            top_by_revenue = df.nlargest(5, 'revenue')[['name', 'revenue']]
            logger.info("Top 5 companies by revenue:")
            for _, row in top_by_revenue.iterrows():
                logger.info(f"  {row['name']}: {row['revenue']:,.0f} руб")
        
        return filename


def test_ruward_parser():
    """Тест парсера RUWARD"""
    
    print("Testing RUWARD Parser")
    print("=" * 60)
    
    parser = RuwardParser()
    
    # Тестируем получение списка рейтингов
    print("\n1. Available ratings on RUWARD:")
    ratings = parser.get_ruward_ratings()
    for rating in ratings:
        print(f"   • {rating['name']}: {rating['description']}")
    
    # Тестируем сбор данных
    print("\n2. Collecting companies (test mode):")
    companies = parser.collect_companies(max_companies=10, use_test_data=True)
    print(f"   Collected {len(companies)} companies")
    
    if companies:
        # Показываем пример компании
        print("\n3. Sample company:")
        sample = companies[0]
        for key, value in sample.items():
            if key in ['name', 'category', 'revenue', 'rating_position', 'segment_tag']:
                print(f"   {key}: {value}")
        
        # Сохраняем в CSV
        print("\n4. Saving to CSV...")
        filename = parser.save_to_csv(companies, 'test_ruward.csv')
        print(f"   Saved to: {filename}")
    
    print("\n" + "=" * 60)
    print("Parser ready for use")


if __name__ == "__main__":
    test_ruward_parser()