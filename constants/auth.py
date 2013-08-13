import base64
import ConfigParser
import os.path

class AuthInfo(object):
    
    def EncodeString(self, str):
        encoded = base64.b64encode(str)
        return encoded
    
    def DecodeString(self, str):
        decoded = base64.b64decode(str)
        return decoded
    
    def GetConfigValue(self, section, option):                
        value = self.config.get(section, option)
        return value
    
    def GetConfigValueInt(self, section, option):
        value = self.config.getint(section, option)
        return value
    
    def __init__(self):
        #config file
        if os.path.exists("rackcli.cfg"):
            self.config_file = "rackcli.cfg"
        elif os.path.exists("../rackcli.cfg"):
            self.config_file = "../rackcli.cfg"
            
        self.config = ConfigParser.RawConfigParser()            
        self.config.read(self.config_file)

        #general
        self.USER_AGENT   = self.GetConfigValue('RACKCLI', 'USER_AGENT')
        self.LOG_FILENAME = self.GetConfigValue('RACKCLI', 'LOG_FILENAME')
        self.DEBUG_LEVEL  = self.GetConfigValueInt('RACKCLI', 'DEBUG_LEVEL')

        #cloud info
        self.ACCOUNT_NUMBER = self.GetConfigValue('CLOUD AUTH', 'ACCOUNT_NUMBER')
        self.USER = self.GetConfigValue('CLOUD AUTH', 'USER')
        self.AUTH_KEY = self.GetConfigValue('CLOUD AUTH', 'KEY')
        self.AUTH_URL = self.GetConfigValue('CLOUD AUTH', 'URL')
        self.AUTH_VERSION = self.GetConfigValue('CLOUD AUTH', 'VERSION')
        
        #queue info
        self.QUEUE_API_VERSION = self.GetConfigValue('QUEUE', 'QUEUE_API_VERSION')
        self.QUEUE_API_URL = self.GetConfigValue('QUEUE', 'QUEUE_API_URL')
        self.QUEUE_XMLNS = self.GetConfigValue('QUEUE', 'QUEUE_XMLNS')


