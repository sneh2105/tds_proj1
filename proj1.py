import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectTimeout, HTTPError, RetryError
from urllib3.util.retry import Retry

# Set your GitHub token here
GITHUB_TOKEN = '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'
headers = {'Authorization': f'token {GITHUB_TOKEN}'}

# Timeout and retry configuration
TIMEOUT = 10  # seconds
RETRIES = 5
BACKOFF_FACTOR = 0.3

# Session setup with retry strategy
session = requests.Session()
retry_strategy = Retry(
    total=RETRIES,
    backoff_factor=BACKOFF_FACTOR,
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

# Quick function to verify GitHub token authentication
def check_token():
    url = 'https://api.github.com/rate_limit'
    try:
        response = session.get(url, headers=headers, timeout=TIMEOUT)
        if response.status_code == 200:
            print("Token authentication successful.")
        else:
            print(f"Authentication failed: {response.status_code} - {response.text}")
    except (ConnectTimeout, RetryError) as e:
        print(f"Connection error: {e}")

# Helper function to clean company names
def clean_company_name(company):
    if company:
        company = company.strip()
        if company.startswith('@'):
            company = company[1:]
        return company.upper()
    return ''

# Function to search for all users in Basel with over 10 followers using pagination
def search_users_in_basel():
    url = 'https://api.github.com/search/users'
    users = []
    page = 1
    while True:
        params = {
            'q': 'location:Basel followers:>10',
            'per_page': 100,
            'page': page
        }
        try:
            response = session.get(url, headers=headers, params=params, timeout=TIMEOUT)
            response.raise_for_status()

            data = response.json()
            retrieved_users = data.get('items', [])
            print(f"Page {page}: Retrieved {len(retrieved_users)} users")  # Debug output

            if not retrieved_users:
                break

            users.extend(retrieved_users)
            
            # Break if fewer than 100 results, indicating the last page
            if len(retrieved_users) < 100:
                break

            page += 1
        
        except (HTTPError, ConnectTimeout) as e:
            print(f"Error retrieving data: {e}")
            break

    return users

# Function to get detailed user information
def get_user_details(username):
    url = f'https://api.github.com/users/{username}'
    try:
        response = session.get(url, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except (HTTPError, ConnectTimeout) as e:
        print(f"Error fetching user {username}: {e}")
        return None

# Function to get up to 500 most recent repositories for a user using pagination
def get_user_repositories(username):
    url = f'https://api.github.com/users/{username}/repos'
    params = {'per_page': 100, 'sort': 'pushed'}
    repos = []
    page = 1
    while len(repos) < 500:
        try:
            response = session.get(url, headers=headers, params={**params, 'page': page}, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            repos.extend(data)
            page += 1
        except (HTTPError, ConnectTimeout) as e:
            print(f"Error fetching repos for {username}: {e}")
            break
    return repos[:500]

# Function to save user data to users.csv
def save_users_to_csv(users):
    user_df = pd.DataFrame(users)
    
    # Ensure 'company' column exists and handle missing data safely
    if 'company' not in user_df.columns:
        user_df['company'] = ''
    user_df['company'] = user_df['company'].apply(clean_company_name)
    
    user_df.to_csv('users.csv', index=False)
    print("User data saved to users.csv")

# Function to save repository data to repositories.csv
def save_repositories_to_csv(repositories):
    repo_df = pd.DataFrame(repositories)
    repo_df.to_csv('repositories.csv', index=False)
    print("Repository data saved to repositories.csv")

# Main function to orchestrate data collection
def main():
    # Check token authentication
    check_token()

    # Fetch all users in Basel with over 10 followers
    users = search_users_in_basel()
    user_data = []
    repo_data = []

    for user in users:
        user_details = get_user_details(user['login'])
        if user_details:
            # Collect user details
            user_data.append({
                'login': user_details['login'],
                'name': user_details.get('name', ''),
                'company': user_details.get('company', ''),
                'location': user_details.get('location', ''),
                'email': user_details.get('email', ''),
                'hireable': user_details.get('hireable', ''),
                'bio': user_details.get('bio', ''),
                'public_repos': user_details['public_repos'],
                'followers': user_details['followers'],
                'following': user_details['following'],
                'created_at': user_details['created_at']
            })

            # Collect repositories for each user
            repositories = get_user_repositories(user['login'])
            for repo in repositories:
                repo_data.append({
                    'login': user_details['login'],
                    'full_name': repo['full_name'],
                    'created_at': repo['created_at'],
                    'stargazers_count': repo['stargazers_count'],
                    'watchers_count': repo['watchers_count'],
                    'language': repo.get('language', ''),
                    'has_projects': repo['has_projects'],
                    'has_wiki': repo['has_wiki'],
                    'license_name': repo['license']['key'] if repo['license'] else ''
                })

    # Save data to CSV files
    save_users_to_csv(user_data)
    save_repositories_to_csv(repo_data)

if __name__ == '__main__':
    main()
