import base64
from hashlib import md5
from loguru import logger


class Encrypt():
    @logger.catch(level='ERROR')
    def __init__(self):
        self.m5_method = {
            0: 'md5($pass)',
            1: 'md5(md5($pass))',
            2: 'md5($pass.$salt)',
            3: 'md5($salt.$pass)',
            4: 'md5($salt.$pass.$salt)',
            5: 'md5(md5($pass).$salt)',
            6: 'md5($salt.md5($pass))',
        }

    @logger.catch(level='ERROR')
    def str2b64(self, target, display=True):
        result = base64.b64encode(target.encode('utf-8')).decode('utf-8')
        if display:
            logger.info(f'{target} base64 encode: {result}')
        return result

    @logger.catch(level='ERROR')
    def b642str(self, target, display=True):
        result = base64.b64decode(target).decode('utf-8')
        if display:
            logger.info(f'{target} base64 decode: {result}')
        return result

    def cmp_b64(self, b64, target):
        if b64 == self.str2b64(target, False):
            logger.success(f'{target} base64 encode is equal to {b64}')
            return True
        else:
            logger.error(f'{target} base64 encode is not equal to {b64}')
            return False

    @logger.catch(level='ERROR')
    def _m5(self, target):
        m5 = md5()
        m5.update(target.encode('utf-8'))
        result = m5.hexdigest()
        return result

    @logger.catch(level='ERROR')
    def md5(self, target, salt=None, display=True):
        m5pass = self._m5(target)
        m5_m5pass = self._m5(m5pass)
        if not salt:
            if display:
                logger.info(f'\n 常见Md5加密算法：\n md5($pass): {m5pass}\n md5(md5($pass)): {m5_m5pass}')
            return m5pass, m5_m5pass, '', '', '', '', ''
        else:
            m5_pass_salt = self._m5(target + salt)
            m5_salt_pass = self._m5(salt+target)
            m5_salt_pass_salt = self._m5(salt+target+salt)
            m5_m5pass_salt = self._m5(m5pass+salt)
            m5_salt_m5pass = self._m5(salt+m5pass)
            if display:
                logger.info(f'\n 常见Md5加密算法：\n md5($pass): {m5pass}\n md5(md5($pass)): {m5_m5pass}\n 常见加盐Salt加密算法：\n md5($pass.$salt): {m5_pass_salt}\n md5($salt.$pass): {m5_salt_pass}\n md5($salt.$pass.$salt): {m5_salt_pass_salt}\n md5(md5($pass).$salt): {m5_m5pass_salt}\n md5($salt.md5($pass)): {m5_salt_m5pass}')
            return m5pass, m5_m5pass, m5_pass_salt, m5_salt_pass, m5_salt_pass_salt, m5_m5pass_salt, m5_salt_m5pass

    @logger.catch(level='ERROR')
    def cmp_md5(self, m5, target, salt=None):
        results = list(self.md5(target, salt, False))
        for inx, result in enumerate(results):
            if m5 == result:
                logger.success(f'[+] {target} md5 match: {m5}, encrypt method: {self.m5_method[inx]}')
                return True
        logger.error(f"[-] {target} md5 not match.")
        return False


if __name__ == '__main__':
    ept = Encrypt()
    ept.str2b64('wkaifeng2007@163.com')
    ept.b642str('d2thaWZlbmcyMDA3QDE2My5jb20=')
    ept.cmp_b64('d2thaWZlbmcyMDA3QDE2My5jb20=', 'wkaifeng2007@163.com')
    ept.cmp_b64('d2thaWZlbmcyMDA3QDE2My5jb20=', 'wkaifeng2007@163.com.cn')
    ept.md5('123456')
    ept.md5('123456', '435229699')
    ept.cmp_md5('8b6de44375f0ad225523c0ee426f95e1', '123456', '435229699')
    ept.cmp_md5('8b6de44375f0ad225523c0ee426f95e1', '123456', '4352296991')
