import httplib2
from kwmqueues.constants.auth import AuthInfo

class QUEUEAPI(object):
    def __init__(self):
        self.http = httplib2.Http()
        auth = AuthInfo()
        
        httplib2.debuglevel=auth.DEBUG_LEVEL
        self.API_URL = auth.QUEUE_API_URL
        self.API_VERSION = auth.QUEUE_API_VERSION
                
        self.USER_AGENT = auth.USER_AGENT
        self.USER = auth.USER
        self.AUTH_KEY = auth.AUTH_KEY
        self.AUTH_URL = auth.AUTH_URL
        self.AUTH_VERSION = auth.AUTH_VERSION
        self.ACCOUNT_NUMBER = auth.ACCOUNT_NUMBER
        
        self.URL_QUEUES = "/queues"

        self.auth_token = None
        
        return
    
    def GetXSignature(self):
        return ""
    
    def GetDefaultHeaders(self):
        return {'Accept': 'application/json',
                    'User-Agent': self.USER_AGENT,
                    'Client-ID': self.USER_AGENT,
                    'X-Auth-Token': self.GetAuthToken(),
                    'X-Project-Id': self.ACCOUNT_NUMBER,
                    'connection': 'close'
                    }

    def GetAuthHeaders(self):
        return {'Accept': 'application/json',
                    'User-Agent': self.USER_AGENT,
                    'X-Auth-User': self.USER,
                    'X-Auth-Key': self.AUTH_KEY,
                    'connection': 'close'
                    }
    
    def GetAuthToken(self):

        if self.auth_token == None:
            url = self.AUTH_URL + self.AUTH_VERSION
            response, content = self.http.request(url, body = None, headers = self.GetAuthHeaders())
        
            if response.status == 204:
                if 'x-auth-token' in response:
                    self.auth_token = response['x-auth-token']
        
        return self.auth_token
    
    def RequestURL(self, url, addDefaults = True):
        if addDefaults:
            url = self.API_URL + self.API_VERSION + url
        
        response, content = self.http.request(url, body = None, headers = self.GetDefaultHeaders())
        
        #retry once if auth failed
        if response.status == 401:
            self.auth_token = None
            response, content = self.http.request(url, body = None, headers = self.GetDefaultHeaders())
        
        return response, content
    
    def PostURL(self, url, postBody):
        url = self.API_URL + self.API_VERSION + url
        response, content = self.http.request(url, method = "POST", body = postBody, headers = self.GetDefaultHeaders())
        
        #retry once if auth failed
        if response.status == 401:
            self.auth_token = None
            response, content = self.http.request(url, method = "POST", body = postBody, headers = self.GetDefaultHeaders())
        
        return response, content        
    
    def PutURL(self, url, postBody):
        url = self.API_URL + self.API_VERSION + url
        response, content = self.http.request(url, method = "PUT", body = postBody, headers = self.GetDefaultHeaders())
        
        #retry once if auth failed
        if response.status == 401:
            self.auth_token = None
            response, content = self.http.request(url, method = "PUT", body = postBody, headers = self.GetDefaultHeaders())
        
        return response, content            

    def DeleteURL(self, url):
        url = self.API_URL + self.API_VERSION + url
        response, content = self.http.request(url, method = "DELETE", headers = self.GetDefaultHeaders())
        
        #retry once if auth failed
        if response.status == 401:
            self.auth_token = None
            response, content = self.http.request(url, method = "DELETE", headers = self.GetDefaultHeaders())
        
        return response, content        
