import requests
import json
import snowflake.connector

def generate_bearer_token():
    api_url = "https://jnj-prod.apigee.net/v3/auth/token"

    headers = {
        "grant_type": "client_credentials",
        "Authorization": "Basic dkxxMW1iSVdDelBoRTBkSlBlZGQ5RUVCYkd0ZTJza1Q6SXNMSEVqNFMxT2xucHEyVQ==" 
    }

    response = requests.post(api_url, headers=headers)

    if response.status_code == 200:
        response_data = response.json()
        bearer_token = response_data.get("bearerToken")
        return bearer_token
    else:
        print("Failed to generate bearer token. Status code:", response.status_code)
        print("Response:", response.text)
        return None

def fetch_jots_data():
    api_url = "https://jnj-prod.apigee.net/v2/auth/discovery"

    bearer_token = generate_bearer_token()

    if not bearer_token:
        return None

    authorization_token = f"Bearer {bearer_token}"

    data_info = {
        "format": "csv",
        "data_source_name": "postgres",
        "data_set_name": "jots_campaign"
    }

    try:
        response = requests.post(api_url, headers={"Authorization": authorization_token}, json=data_info)

        if response.status_code == 200:
            return response.text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print("Response:", response.text)
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

# Example usage
data = fetch_jots_data()
if data:
    print("Data fetched successfully:")
    print(data)
else:
    print("Failed to fetch data.")
