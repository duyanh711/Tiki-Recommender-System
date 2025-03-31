import urllib.request

def download_html(url):

    with urllib.request.urlopen(url) as response:
        return response.read().decode('utf-8')