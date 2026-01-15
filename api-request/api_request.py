import requests

api_url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"

def fetch_data():
    print("Fetching scoreboard data from ESPN API...")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        print("API response received successfully.")
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"An error occured: {e}")
        raise