import secrets as s

base_url = 'https://na1.api.riotgames.com'
MATCH = '/lol/match/v4/matches/'
MATCHLIST = '/lol/match/v4/matchlists/by-account/'

def_q_params = {'api_key':s.api_key}
Ml_q_params = {'champions':'',
               'queue':'',
               'season':'',
               'endTime':'',
               'beginTime':'',
               'endIndex':'',
               'beginIndex':''}
