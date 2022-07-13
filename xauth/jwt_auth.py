import time
from fastapi import Request
import xauth.verifier as verify
from exceptions.customs import InvalidPermissions, UnauthorizedAPIRequest, RecordNotFound


class JwtAuth():
    def __init__(self):
        self.identities = ['dev', 'prod', 'antx']
        self.salt = '9_0^9_1'

    def set_config(self, identities: list=[], salt: str=None):
        if identities:
            self.identities = identities
        if salt is not None:
            self.salt = salt

    def auth_required(self, request):
        """
        权限验证（用户必须有相关权限才能通过验证）
        :param function_to_protect:
        :return:
        """
        # 获取token 内容
        status, auth_token = verify.verify_request(request)
        if not status:
            raise UnauthorizedAPIRequest("Verify token failed, rejecting request!")
        exp_time = auth_token['exp']
        identify = auth_token['identity']
        if identify not in self.identities:
            raise InvalidPermissions('Unauthorized user, rejecting request!')
        if int(exp_time) < int(time.time()):
            raise UnauthorizedAPIRequest('Token was expired, Please re-login!')
        return True

    def verification(self, request: Request):
        return self.auth_required(request=request)
