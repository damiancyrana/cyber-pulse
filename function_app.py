import logging
import os
import re
import json
import certifi
import requests
import cloudscraper
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from azure.storage.blob import BlobServiceClient
import azure.functions as func

# Set SSL certificate
os.environ['SSL_CERT_FILE'] = certifi.where()


class ScraperStrategy:
    def fetch_html(self, url):
        pass

    def extract_articles(self, html_content, today_date):
        pass

    def fetch_article_content(self, article_url):
        pass


class DarkReadingScraper(ScraperStrategy):
    def __init__(self, base_url):
        self.base_url = base_url

    def fetch_html(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.text

    def extract_articles(self, html_content, today_date):
        soup = BeautifulSoup(html_content, 'html.parser')
        latest_news_section = soup.find('div', class_='LatestFeatured-ColumnList LatestFeatured-ColumnList_left', attrs={'data-testid': 'list-content'})

        if not latest_news_section:
            logging.info("Nie znaleziono sekcji 'Latest News'.")
            return []

        articles = latest_news_section.find_all('div', class_='ContentPreview LatestFeatured-ContentItem LatestFeatured-ContentItem_left')

        news_list = []
        for article in articles:
            title_tag = article.find('a', class_='ListPreview-Title', attrs={'data-testid': 'preview-default-title'})
            date_tag = article.find('span', class_='ListPreview-Date', attrs={'data-testid': 'list-preview-date'})
            link_tag = article.find('a', class_='ListPreview-Title')
            author_tag = article.find('a', class_='Contributors-ContributorName', attrs={'data-testid': 'contributor-name'})
            tags_wrapper = article.find('div', class_='ListPreview-KeywordWrapper')

            if not title_tag or not date_tag or not link_tag:
                continue

            tags = ", ".join(tag.text.strip() for tag in tags_wrapper.find_all('a', class_='Keyword')) if tags_wrapper else ""
            article_date = datetime.strptime(date_tag.text.strip(), "%b %d, %Y").date()

            if article_date == today_date:
                news_list.append({
                    'title': title_tag.text.strip(),
                    'date': date_tag.text.strip(),
                    'link': urljoin(self.base_url, link_tag['href']),
                    'author': author_tag.text.strip() if author_tag else 'Unknown',
                    'tags': tags,
                    'content': ''
                })
        return news_list

    def fetch_article_content(self, article_url):
        article_html = self.fetch_html(article_url)
        soup = BeautifulSoup(article_html, 'html.parser')
        content_section = soup.find('div', class_='ArticleBase-BodyContent ArticleBase-BodyContent_Article', attrs={'data-testid': 'article-base-body-content'})

        if not content_section:
            logging.info(f"Nie znaleziono treści artykułu na stronie: {article_url}")
            return ""

        paragraphs = content_section.find_all('p', class_='ContentParagraph')
        article_content = " ".join(paragraph.text.strip().replace('"', '') for paragraph in paragraphs if paragraph)
        return article_content


class AzureBlobSaver:
    def __init__(self, connection_string, container_name):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = container_name

    def save_to_blob_storage(self, data, blob_name):
        container_client = self.blob_service_client.get_container_client(self.container_name)
        if not container_client.exists():
            container_client.create_container()

        json_data = json.dumps(data, indent=4, ensure_ascii=False)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_data, overwrite=True)
        logging.info(f"Dane zostały zapisane w blob: {blob_name} w kontenerze: {self.container_name}")


# Azure Function Timer Trigger
app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 */12 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    logging.info('Rozpoczęcie działania Azure Function.')

    if myTimer.past_due:
        logging.info('The timer is past due!')

    today_date = datetime.now().date()

    darkreading_base_url = 'https://www.darkreading.com/'
    hackernews_base_url = 'https://thehackernews.com/'

    # Scraping danych
    darkreading_scraper = DarkReadingScraper(darkreading_base_url)
    hackernews_scraper = DarkReadingScraper(hackernews_base_url)

    darkreading_html = darkreading_scraper.fetch_html(darkreading_base_url)
    hackernews_html = hackernews_scraper.fetch_html(hackernews_base_url)

    darkreading_articles = darkreading_scraper.extract_articles(darkreading_html, today_date)
    hackernews_articles = hackernews_scraper.extract_articles(hackernews_html, today_date)

    for article in darkreading_articles:
        logging.info(f"Pobieranie treści dla artykułu: {article['title']}")
        article['content'] = darkreading_scraper.fetch_article_content(article['link'])

    for article in hackernews_articles:
        logging.info(f"Pobieranie treści dla artykułu: {article['title']}")
        article['content'] = hackernews_scraper.fetch_article_content(article['link'])

    combined_articles = {
        "thehackernews": hackernews_articles,
        "darkreading": darkreading_articles
    }

    # Zapis do Azure Blob Storage
    azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "default-container")
    blob_name = f"{today_date}_cyber_articles.json"

    azure_saver = AzureBlobSaver(azure_connection_string, container_name)
    azure_saver.save_to_blob_storage(combined_articles, blob_name)

    logging.info('Zakończono działanie Azure Function')
    