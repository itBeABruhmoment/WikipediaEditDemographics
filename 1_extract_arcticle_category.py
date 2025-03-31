import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import csv
import os

def extract_articles_from_category(category_url):
    """
    Extract all article links from a Wikipedia category page.
    
    Args:
        category_url (str): URL of the Wikipedia category page
        
    Returns:
        list: List of tuples containing (article title, article URL, category_url)
    """
    print(f"Processing: {category_url}")
    
    # Make the request with a user agent to avoid being blocked
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(category_url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to retrieve the page: {response.status_code}")
            return []
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the div with id 'mw-pages'
        mw_pages = soup.find('div', id='mw-pages')
        
        if not mw_pages:
            print(f"No articles found in category: {category_url}")
            return []
        
        # Find all links within the div
        article_links = []
        
        # Find all category groups
        category_groups = mw_pages.find_all('div', class_='mw-category-group')
        
        # Get category name for reference
        category_name = soup.find('h1', id='firstHeading').text if soup.find('h1', id='firstHeading') else category_url
        
        # If there are no specific groups, look for links directly
        if not category_groups:
            for link in mw_pages.find_all('a'):
                href = link.get('href')
                title = link.text
                
                # Filter out non-article links
                if href and href.startswith('/wiki/') and ':' not in href:
                    full_url = f"https://en.wikipedia.org{href}"
                    article_links.append((title, full_url, category_url, category_name))
        else:
            # Process each category group
            for group in category_groups:
                for link in group.find_all('a'):
                    href = link.get('href')
                    title = link.text
                    
                    # Filter out non-article links
                    if href and href.startswith('/wiki/') and ':' not in href:
                        full_url = f"https://en.wikipedia.org{href}"
                        article_links.append((title, full_url, category_url, category_name))
        
        # Check for pagination - "next page" link
        next_page = None
        if mw_pages:
            for a in mw_pages.find_all('a'):
                if a.text == 'next page' or a.text == 'next 200':
                    next_page = 'https://en.wikipedia.org' + a['href']
                    break
        
        # Recursively get articles from next pages if they exist
        if next_page:
            print(f"Following next page: {next_page}")
            # Add a small delay to avoid overloading the server
            time.sleep(1)
            next_page_articles = extract_articles_from_category(next_page)
            article_links.extend(next_page_articles)
        
        return article_links
    
    except Exception as e:
        print(f"Error processing {category_url}: {e}")
        return []

def process_categories_from_csv():
    """
    Process all category URLs from a CSV file and save results to a new CSV.
    """
    input_file = "./articles/categories.csv"
    output_file = "./articles/category_articles.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Input file {input_file} not found.")
        return
    
    # Read category URLs from CSV
    try:
        df = pd.read_csv(input_file)
        if 'url' not in df.columns:
            print("CSV file must have a 'url' column.")
            return
        
        # Extract category URLs
        category_urls = df['url'].tolist()
        
        # Create CSV file with headers
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['article_title', 'article_url', 'category_url', 'category_name'])
        
        # Process each category URL
        all_articles = []
        for url in category_urls:
            # Add a delay between requests to avoid being blocked
            time.sleep(2)
            articles = extract_articles_from_category(url)
            all_articles.extend(articles)
            
            # Write batch to CSV to avoid losing data if script crashes
            with open(output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for article in articles:
                    writer.writerow(article)
            
            print(f"Saved {len(articles)} articles from {url}")
        
        print(f"Total articles saved: {len(all_articles)}")
        print(f"Results saved to {output_file}")
        
    except Exception as e:
        print(f"Error processing categories: {e}")

if __name__ == "__main__":
    process_categories_from_csv()