import requests
import urllib.parse
import pandas as pd
import os
from datetime import datetime
import sys
import time

def extract_title_from_url(url):
    """
    Extract Wikipedia article title from a full URL.
    :param url: Full Wikipedia page URL
    :return: Decoded article title
    """
    # Parse the URL
    parsed_url = urllib.parse.urlparse(url)
    
    # Check if it's a valid Wikipedia URL
    if not (parsed_url.netloc.endswith('wikipedia.org') and parsed_url.path.startswith('/wiki/')):
        raise ValueError("Invalid Wikipedia URL. Please provide a full Wikipedia page URL.")
    
    # Extract and decode the title from the path
    title = parsed_url.path.split('/wiki/')[1]
    return urllib.parse.unquote(title)

def get_wikipedia_article_history(url, limit=500):
    """
    Retrieve the revision history of a Wikipedia article from a URL.
    :param url: Full Wikipedia page URL
    :param limit: Maximum number of revisions to retrieve (default 50)
    :return: List of revision details
    """
    # Extract article title from the URL
    try:
        article_title = extract_title_from_url(url)
        
        # Wikipedia API endpoint
        base_url = "https://en.wikipedia.org/w/api.php"
        
        # Parameters for the API request
        params = {
            "action": "query",
            "format": "json",
            "prop": "revisions",
            "titles": article_title,
            # "rvprop": "ids|timestamp|user|comment|size|tags|content",
            "rvprop": "ids|timestamp|user|comment|size|tags",
            "rvlimit": limit
        }
        
        # Add user agent to avoid being blocked
        headers = {
            'User-Agent': 'WikipediaRevisionHistoryFetcher/1.0 (Research project; contact@example.com)'
        }
        
        # Send the API request
        response = requests.get(base_url, params=params, headers=headers)
        data = response.json()
        
        # Extract the page information
        page = next(iter(data['query']['pages'].values()))
        
        # Check if revisions exist
        if 'revisions' not in page:
            print(f"No revision history found for {article_title}")
            return []
        
        # Process and return revision details
        revisions = page['revisions']
        return [
            {
                'url': url,
                'rev_id': rev.get('revid', 'N/A'),
                'timestamp': rev.get('timestamp', 'N/A'),
                'user': rev.get('user', 'N/A'),
                'comment': rev.get('comment', 'No comment'),
                'size': rev.get('size', 0),
                'tags': ', '.join(rev.get('tags', [])) if rev.get('tags') else 'N/A',
                # 'content': rev.get('*', 'Content not available')  # Use '*' to get content
            }
            for rev in revisions
        ]
    
    except requests.RequestException as e:
        print(f"Error fetching Wikipedia article history for {url}: {e}")
        return []
    except (KeyError, StopIteration) as e:
        print(f"Error parsing Wikipedia API response for {url}: {e}")
        return []
    except ValueError as e:
        print(f"Error with URL {url}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error processing {url}: {e}")
        return []

def save_history_to_csv(history, url):
    """
    Save the article revision history to a CSV file using pandas.
    :param history: List of revision details
    :param url: Original Wikipedia URL
    :return: Filename or None on failure
    """
    if not history:
        print(f"No history to save for {url}.")
        return None
    
    # Create a 'wikipedia_histories' directory if it doesn't exist
    os.makedirs('wikipedia_histories', exist_ok=True)
    
    # Extract article title from the URL for filename
    try:
        article_title = extract_title_from_url(url)
        
        # Generate a safe filename
        safe_title = ''.join(c if c.isalnum() or c in ['-', '_'] else '_' for c in article_title)
        filename = f"wikipedia_histories/{safe_title}.csv"
        
        # Convert to DataFrame
        df = pd.DataFrame(history)
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"Revision history saved to {filename}")
        return filename
    
    except Exception as e:
        print(f"Error saving CSV file for {url}: {e}")
        return None

def process_articles_from_csv(csv_file="articles.csv", limit=500):
    """
    Process all Wikipedia article URLs from a CSV file and save their revision histories.
    :param csv_file: Path to CSV file containing article URLs
    :param limit: Maximum number of revisions to fetch per article
    """
    try:
        # Check if input file exists
        if not os.path.exists(csv_file):
            print(f"Input file {csv_file} not found.")
            return
        
        # Read article URLs from CSV
        df = pd.read_csv(csv_file)
        
        if 'article_url' not in df.columns:
            print("CSV file must have a 'url' column.")
            return
        
        # Extract article URLs
        article_urls = df['article_url'].tolist()
        print(f"Found {len(article_urls)} articles to process.")
        
        # Create results DataFrame to track progress
        results = []
        
        # Process each article URL
        for i, url in enumerate(article_urls):
            print(f"\n[{i+1}/{len(article_urls)}] Processing: {url}")
            
            try:
                # Get article history
                history = get_wikipedia_article_history(url, limit)
                
                # Save history to CSV
                filename = save_history_to_csv(history, url)
                
                # Record result
                results.append({
                    'url': url,
                    'status': 'Success' if filename else 'Failed',
                    'revisions': len(history),
                    'filename': filename
                })
                
                # Add a delay to be nice to Wikipedia servers
                time.sleep(1)
                
            except Exception as e:
                print(f"Error processing {url}: {e}")
                results.append({
                    'url': url,
                    'status': 'Error',
                    'revisions': 0,
                    'filename': None
                })
        
        # Save processing results to CSV
        results_df = pd.DataFrame(results)
        results_df.to_csv("article_processing_results.csv", index=False)
        print(f"\nProcessing complete. Results saved to article_processing_results.csv")
        
    except Exception as e:
        print(f"Error processing articles from CSV: {e}")

# Main execution
if __name__ == "__main__":
    # If arguments are provided, use them as the CSV file name and limit
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 500
        process_articles_from_csv(csv_file, limit)
    else:
        # Use default values
        process_articles_from_csv("./articles/articles.csv", 500)