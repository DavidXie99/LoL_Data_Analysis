import requests
from time import time, sleep

import config as c
import secrets as s

def getReq(base_url,
           api_url,
           path_param,
           query_params,
           rate_limit_override=False,
           success_log=True):
    start = time()

    query_param_string = ''
    if len(query_params):
        query_param_string = '?'+'&'.join(str(p)+'='+str(query_params[p]) for p in query_params if len(str(query_params[p])))
    
    request_string = '{url_1}{url_2}{p_param}{q_param}'.format(url_1=base_url,
                                                               url_2=api_url,
                                                               p_param=path_param,
                                                               q_param=query_param_string)
    #print(request_string)
    try:
        response = requests.get(request_string)
        num_rle = int(response.status_code==429)
        
        while response.status_code == 429 and \
              rate_limit_override:
            sleep(int(response.headers['Retry-After']))
            num_rle += 1
            print(num_rle)
            response = requests.get(request_string)

        num_ise = 1
        while response.status_code//100 == 5 and \
              num_ise < 5:
            print('Server error retrying in 30s')
            sleep(30)
            num_ise += 1
            response = requests.get(request_string)
            
        if success_log:
            print('API:',api_url,
                  #' Path value:',path_param,
                  ' Response time:',round(time()-start,5),
                  '\t Response code:',response.status_code,
                  ' #RLE:',num_rle)
        return response
    
    except requests.exceptions.RequestException as e:
        print('Error during request to {request_url} : {error}'.format(request_url=request_string,
                                                                       error=str(e)))
        return None


