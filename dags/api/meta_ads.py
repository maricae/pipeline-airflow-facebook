import requests
import json
import pandas as pd

class MetaAPI:
    def __init__(self, meta_token, ad_acc):
        self.base_url = "https://graph.facebook.com/v19.0/"
        self.token = f"&access_token={meta_token}"
        self.ad_acc = ad_acc

    def adsets_status(self, status_fields):
        url = self.base_url + 'act_' + str(self.ad_acc)
        url += '/adsets?fields=' + ','.join(status_fields)
        url += '&effective_status=%5B%22ACTIVE%22%5D'
        adsets_json = requests.get(url + self.token)

        return adsets_json
    
    def adsets_content(self, content_fields, level='adset'):
        url = self.base_url + 'act_' + str(self.ad_acc)
        url += '/insights?level=' + level
        url += '&fields=' + ','.join(content_fields) + '&limit=2000'
        content_json = requests.get(url + self.token)

        return content_json
    
    def adsets_actions(self, actions_fields, level='adset'):
        url = self.base_url + 'act_' + str(self.ad_acc)
        url += '/insights?level=' + level
        url += '&fields=' + ','.join(actions_fields) + '&limit=2000'
        actions_json = requests.get(url + self.token)

        return actions_json
    
if __name__ == "__main__": 
    import os
    from dotenv import load_dotenv

    load_dotenv() 
    
    meta_token = os.getenv('meta_token')
    ad_acc = os.getenv('ad_acc')
    status_fields = ['id','status','objective','created_time']

    meta_api = MetaAPI(meta_token, ad_acc)
    status_json = meta_api.adsets_status(status_fields)
    status_json = json.loads(status_json._content.decode('utf-8'))
    status = pd.json_normalize(status_json['data'])

    print(status)


