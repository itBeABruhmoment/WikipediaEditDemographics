import requests
import urllib.parse

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

def get_wikipedia_article_history(url, limit=50):
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
                'rev_id': rev.get('revid', 'N/A'),
                'timestamp': rev.get('timestamp', 'N/A'),
                'user': rev.get('user', 'N/A'),
                'comment': rev.get('comment', 'No comment'),
                'size': rev.get('size', 0),
                'tags': rev.get('tags', [])
            }
            for rev in revisions
        ]
    
    except requests.RequestException as e:
        print(f"Error fetching Wikipedia article history: {e}")
        return []
    except (KeyError, StopIteration) as e:
        print(f"Error parsing Wikipedia API response: {e}")
        return []

def print_article_history(history):
    """
    Print the article revision history in a readable format.
    
    :param history: List of revision details
    """
    if not history:
        print("No revision history to display.")
        return
    
    print(f"{'Revision ID':<15} {'Timestamp':<25} {'User':<20} {'Comment':<30} {'Size':<10} {'Tags'} {'Content'}")
    print("-" * 110)
    
    for revision in history:
        # Truncate tags to prevent overwhelming output
        tags = ', '.join(revision['tags'][:3]) if revision['tags'] else 'N/A'
        
        print(f"{str(revision['rev_id']):<15} "
              f"{revision['timestamp']:<25} "
              f"{revision['user'][:20]:<20} "
              f"{revision['comment'][:30]:<30} "
              f"{str(revision['size']):<10} "
              f"{tags}")

# Example usage
if __name__ == "__main__":
    # Example Wikipedia URLs
    example_urls = [
        "https://en.wikipedia.org/wiki/Knapsack_problem"
    ]
    
    # Fetch and print history for each URL
    for url in example_urls:
        print(f"\nRevision History for: {url}")
        history = get_wikipedia_article_history(url)
        print_article_history(history)

        