# app/utils/http_headers.py

USER_AGENT = "ClimaStationBot/1.0 (contact: your_email@example.com)"

def default_headers():
    return {"User-Agent": USER_AGENT}
