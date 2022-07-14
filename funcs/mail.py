from loguru import logger
import yagmail

class Mail():
    def __init__(self):
        self.sender = ''
        self.receiver = []
        self.host_server = 'smtp.163.com'
        self.sender_password = ''

    @logger.catch(level='ERROR')
    def set_config(self, sender: str, receivers: list, password: str, host_server: str = 'smtp.163.com'):
        if not sender or not receivers or not password:
            raise ValueError('Please provide correct config for email!')
        self.sender = sender
        self.receiver = receivers
        self.sender_password = password
        self.host_server = host_server

    # 发送邮件
    @logger.catch(level='ERROR')
    def email(self, title, content):
        sender_email = self.sender
        sender_pwd = self.sender_password
        host_server = self.host_server
        receiver = self.receiver
        yag = yagmail.SMTP(user=sender_email, password=sender_pwd, host=host_server)
        yag.send(to=receiver, subject=title, contents=content)
