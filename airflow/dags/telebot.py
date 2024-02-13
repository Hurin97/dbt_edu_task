import requests

class Telegram_log:

    def __init__(self, token, chat_id) -> None:
        self.token = token
        self.chat_id = chat_id

    def logger(self, message)->None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage?chat_id={self.chat_id}&text={message}"
        requests.get(url).json()