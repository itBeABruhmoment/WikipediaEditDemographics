import requests
import urllib.parse
import pandas as pd
import os
from datetime import datetime
import sys

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
    
    try:
        # Send the API request
        response = requests.get(base_url, params=params)
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
        print(f"Error fetching Wikipedia article history: {e}")
        return []
    except (KeyError, StopIteration) as e:
        print(f"Error parsing Wikipedia API response: {e}")
        return []

def save_history_to_csv(history, url):
    """
    Save the article revision history to a CSV file using pandas.
    :param history: List of revision details
    :param url: Original Wikipedia URL
    """
    if not history:
        print("No history to save.")
        return None
    
    # Create a 'wikipedia_histories' directory if it doesn't exist
    os.makedirs('wikipedia_histories', exist_ok=True)
    
    # Extract article title from the URL for filename
    article_title = extract_title_from_url(url)
    
    # Generate a filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_title = ''.join(c if c.isalnum() or c in ['-', '_'] else '_' for c in article_title)
    filename = f"wikipedia_histories/{safe_title}.csv"
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(history)
        
        # Save to CSV
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"Revision history saved to {filename}")
        return filename
    
    except Exception as e:
        print(f"Error saving CSV file: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Example Wikipedia URLs
    # example_urls = [
    #     # "https://en.wikipedia.org/wiki/Knapsack_problem",
    #     # "https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm"
    #     "https://en.wikipedia.org/wiki/Dublin"
    # ]
    
    # # Fetch and save history for each URL
    # for url in example_urls:
    url = sys.argv[1]
    print(f"\nFetching Revision History for: {url}")
        
        # Get article history
    history = get_wikipedia_article_history(url)
        
        # Save history to CSV
    save_history_to_csv(history, url)