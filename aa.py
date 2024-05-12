import csv
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from newspaper import Article
import requests
from bs4 import BeautifulSoup
import re
import nltk
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import dvc.api

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for extracting, transforming, and loading Dawn articles',
    schedule_interval='@daily',
)

def extract_articles():
    dawn_url = 'https://www.dawn.com/'
    headers = {
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    response = requests.get(dawn_url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        article_links = []

        # Find the div with the specified class
        article_div = soup.find('div', class_='container mx-auto px-2 sm:px-6.5')

        # Find all anchor tags within the div
        if article_div:
            anchors = article_div.find_all('a', href=True)
            for anchor in anchors[:10]:  # Limiting to 10 articles for example
                article_links.append(anchor['href'])

        return article_links
    else:
        print("Failed to fetch the page:", response.status_code)
        return None

def clean_text(text):
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words('english'))
    tokens = [token for token in tokens if token not in stop_words]
    preprocessed_text = ' '.join(tokens)
    return preprocessed_text

def process_articles(**kwargs):
    ti = kwargs['ti']
    article_links = ti.xcom_pull(task_ids='extract_articles')
    articles_data = []

    for link in article_links:
        try:
            article = Article(link)
            article.download()
            article.parse()
            title = article.title
            description = article.meta_description if article.meta_description else article.summary
            cleaned_description = clean_text(description)
            articles_data.append({'title': title, 'description': cleaned_description, 'link': link})
            print("\nTitle:", title)
            print("Description before cleaning:", description)
            print("Description after cleaning:", cleaned_description)
        except Exception as e:
            print("Error scraping article:", e)

    return articles_data

def load_to_csv(**kwargs):
    ti = kwargs['ti']
    articles_data = ti.xcom_pull(task_ids='process_articles')
    filename = 'dawn_articles.csv'  # Specify the path where you want to save the CSV file

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Description', 'Link', 'Source'])

        for article in articles_data:
            writer.writerow([article['title'], article['description'], article['link'], 'Dawn.com'])

    # Add the CSV file to DVC
    dvc_file_path = os.path.join(os.getcwd(), filename)
    os.system(f"dvc add {dvc_file_path}")
    os.system("dvc commit")

    # Push the data to Google Drive using DVC
    os.system("dvc push -r myremote")

extract_articles_task = PythonOperator(
    task_id='extract_articles',
    python_callable=extract_articles,
    dag=dag,
)

process_articles_task = PythonOperator(
    task_id='process_articles',
    python_callable=process_articles,
    provide_context=True,
    dag=dag,
)

load_to_csv_task = PythonOperator(
    task_id='load_to_csv',
    python_callable=load_to_csv,
    provide_context=True,
    dag=dag,
)

extract_articles_task >> process_articles_task >> load_to_csv_task
