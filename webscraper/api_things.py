import requests
from time import time, sleep
import config as c
import secrets as s


def getReq(base_url,
           api_url,
           path_param,
           query_params,
           success_log=True):
    start = time()

    query_param_string = ''
    if len(query_params):
        query_param_string = '?' + '&'.join(
            str(p) + '=' + str(query_params[p]) for p in query_params if len(str(query_params[p])))

    request_string = '{url_1}{url_2}{p_param}{q_param}'.format(url_1=base_url,
                                                               url_2=api_url,
                                                               p_param=path_param,
                                                               q_param=query_param_string)
    try:
        response = requests.get(request_string)
        num_rle = int(response.status_code == 429)

        while int(response.status_code) == 429:
            try:
                print('#RLE:', num_rle, '\t Retrying in', response.headers['Retry-After'])
                sleep(int(response.headers['Retry-After']))
            except Exception as e:
                print('Error occurred in 429 handling: ', e)
                sleep(10)
            num_rle += 1
            response = requests.get(request_string)

        num_ise = 1
        while response.status_code // 100 == 5 and \
                num_ise < 5:
            print('Server error retrying in {sec}s'.format(sec=5 * num_ise))
            sleep(5 * num_ise)
            num_ise += 1
            response = requests.get(request_string)

        if success_log:
            print('API:', api_url[4:],
                  'param:', path_param,
                  '\ttime:', round(time() - start, 5),
                  '\tsc:', response.status_code)
        return response

    except requests.exceptions.RequestException as e:
        print('Error during request to {request_url} : {error}'.format(request_url=request_string,
                                                                       error=str(e)))
        return None
