from utils.clio_client import AuthClient
from flask import Flask, request, redirect

app = Flask(__name__)


@app.route("/auth")
def auth():
    client = AuthClient()
    url = client.get_authorization_url()
    return redirect(url)


@app.route("/callback")
def callback():
    client = AuthClient()
    url = request.url.replace("http:", "https:")
    oauth_token = client.get_token(url)
    client.save_tokens(oauth_token)
    return "OK"


@app.route("/health")
def health():
    return "OK"
