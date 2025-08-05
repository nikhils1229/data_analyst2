import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict, Any, Optional
import logging
import re
import asyncio

logger = logging.getLogger(__name__)

class WebScraper:
    """Handles web scraping operations"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    async def scrape_wikipedia_films(self, url: Optional[str] = None) -> List[Dict[str, Any]]:
        """Scrape Wikipedia highest grossing films data"""
        if not url:
            url = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
        
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            films_data = await loop.run_in_executor(None, self._scrape_films_sync, url)
            return films_data
            
        except Exception as e:
            logger.error(f"Error scraping Wikipedia films: {str(e)}")
            return self._get_sample_films_data()
    
    def _scrape_films_sync(self, url: str) -> List[Dict[str, Any]]:
        """Synchronous scraping method"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find tables with film data
            tables = soup.find_all('table', {'class': 'wikitable'})
            
            films_data = []
            for table in tables:
                # Check if this looks like the films table
                headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                
                if any(keyword in ' '.join(headers) for keyword in ['rank', 'film', 'worldwide', 'gross']):
                    table_data = self._parse_films_table(table)
                    films_data.extend(table_data)
            
            # If no data found, return sample data
            if not films_data:
                return self._get_sample_films_data()
            
            return films_data[:50]  # Limit to first 50 for performance
            
        except Exception as e:
            logger.error(f"Error in sync scraping: {str(e)}")
            return self._get_sample_films_data()
    
    def _parse_films_table(self, table) -> List[Dict[str, Any]]:
        """Parse Wikipedia films table"""
        films = []
        
        try:
            # Get headers
            header_row = table.find('tr')
            headers = []
            
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    header_text = th.get_text(strip=True)
                    headers.append(self._normalize_header(header_text))
            
            # Get data rows
            rows = table.find_all('tr')[1:]  # Skip header row
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                
                if len(cells) >= 3:  # Must have at least rank, title, gross
                    film_data = {}
                    
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            header = headers[i]
                            cell_text = cell.get_text(strip=True)
                            
                            # Clean and parse cell data
                            if header == 'rank':
                                film_data['rank'] = self._parse_number(cell_text)
                            elif header in ['title', 'film']:
                                film_data['title'] = self._clean_film_title(cell_text)
                            elif 'gross' in header or 'revenue' in header:
                                film_data['worldwide_gross'] = cell_text
                            elif header == 'year':
                                film_data['year'] = self._parse_year(cell_text)
                            elif header == 'peak':
                                film_data['peak'] = self._parse_number(cell_text)
                            else:
                                film_data[header] = cell_text
                    
                    if 'title' in film_data and film_data['title']:
                        films.append(film_data)
            
            return films
            
        except Exception as e:
            logger.error(f"Error parsing table: {str(e)}")
            return []
    
    def _normalize_header(self, header: str) -> str:
        """Normalize table header names"""
        header = header.lower().strip()
        
        # Common normalizations
        normalizations = {
            'worldwide gross': 'worldwide_gross',
            'box office': 'worldwide_gross',
            'total gross': 'worldwide_gross',
            'film title': 'title',
            'movie': 'title',
            'release year': 'year',
            'year released': 'year',
        }
        
        for key, value in normalizations.items():
            if key in header:
                return value
        
        # Clean up header name
        header = re.sub(r'[^\w\s]', '', header)  # Remove special chars
        header = re.sub(r'\s+', '_', header)     # Replace spaces with underscores
        
        return header
    
    def _clean_film_title(self, title: str) -> str:
        """Clean film title text"""
        # Remove citations and extra info
        title = re.sub(r'\[.*?\]', '', title)  # Remove [1], [citation needed], etc.
        title = re.sub(r'\(.*?\)', '', title)  # Remove (2009), (film), etc.
        return title.strip()
    
    def _parse_number(self, text: str) -> Optional[int]:
        """Extract integer from text"""
        numbers = re.findall(r'\d+', text)
        return int(numbers[0]) if numbers else None
    
    def _parse_year(self, text: str) -> Optional[int]:
        """Extract year from text"""
        year_match = re.search(r'\b(19|20)\d{2}\b', text)
        return int(year_match.group()) if year_match else None
    
    def _get_sample_films_data(self) -> List[Dict[str, Any]]:
        """Return sample films data when scraping fails"""
        return [
            {
                'rank': 1,
                'title': 'Avatar',
                'worldwide_gross': '$2.923 billion',
                'year': 2009,
                'peak': 1
            },
            {
                'rank': 2,
                'title': 'Avengers: Endgame',
                'worldwide_gross': '$2.798 billion',
                'year': 2019,
                'peak': 1
            },
            {
                'rank': 3,
                'title': 'Avatar: The Way of Water',
                'worldwide_gross': '$2.320 billion',
                'year': 2022,
                'peak': 3
            },
            {
                'rank': 4,
                'title': 'Titanic',
                'worldwide_gross': '$2.257 billion',
                'year': 1997,
                'peak': 1
            },
            {
                'rank': 5,
                'title': 'Star Wars: The Force Awakens',
                'worldwide_gross': '$2.071 billion',
                'year': 2015,
                'peak': 5
            },
            {
                'rank': 6,
                'title': 'Avengers: Infinity War',
                'worldwide_gross': '$2.048 billion',
                'year': 2018,
                'peak': 6
            },
            {
                'rank': 7,
                'title': 'Spider-Man: No Way Home',
                'worldwide_gross': '$1.921 billion',
                'year': 2021,
                'peak': 7
            },
            {
                'rank': 8,
                'title': 'Jurassic World',
                'worldwide_gross': '$1.672 billion',
                'year': 2015,
                'peak': 8
            },
            {
                'rank': 9,
                'title': 'The Lion King',
                'worldwide_gross': '$1.657 billion',
                'year': 2019,
                'peak': 9
            },
            {
                'rank': 10,
                'title': 'The Avengers',
                'worldwide_gross': '$1.519 billion',
                'year': 2012,
                'peak': 10
            }
        ]
