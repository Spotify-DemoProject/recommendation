
def create_auth_code(client_id:str):
    from urllib.parse import urlencode
    import webbrowser

    auth_headers = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": "http://localhost:7777/callback",
        "scope": "playlist-modify-private"
    }

    webbrowser.open("https://accounts.spotify.com/authorize?" + urlencode(auth_headers))

def get_auth_token(auth_code:str, client_id:str, client_sc:str):
    import base64, requests, json

    encoded_credentials = base64.b64encode(client_id.encode() + b':' + client_sc.encode()).decode("utf-8")

    token_headers = {
        "Authorization": "Basic " + encoded_credentials,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    token_data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": "http://localhost:7777/callback"
    }
    r = requests.post("https://accounts.spotify.com/api/token", data=token_data, headers=token_headers)
    # print(r.json())
    token = r.json()["access_token"]
    
    return token

def get_access_token(client_id:str, client_sc:str):
    import requests
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_sc}'.encode()
    response = requests.post('https://accounts.spotify.com/api/token', headers=headers, data=data).json()
    access_token = response['access_token']

    return access_token

def get_response(access_token:str, endpoint:str, params:dict=None):
    import requests

    url = f"https://api.spotify.com/v1/{endpoint}"
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    if params != None:
        response = requests.get(url=url, params=params, headers=headers)
    else:
        response = requests.get(url=url, headers=headers)
    print(response)
    return response.json()

def post_response(auth_token:str, endpoint:str, data:dict=None):
    import requests, json

    url = f"https://api.spotify.com/v1/{endpoint}"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url=url, headers=headers, data=json.dumps(data))
    print(response)
    return response.json()

if __name__ == "__main__":
    create_auth_code(client_id="")