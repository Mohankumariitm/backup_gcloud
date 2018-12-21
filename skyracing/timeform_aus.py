import requests
import grequests
import traceback
import time
from pprint import pprint
from bs4 import BeautifulSoup
import dateutil.parser
from datetime import timedelta
import datetime
import pytz
import pickle
import json

model_name="timeform_aus"

try:
  junk=requests.put("http://localhost:9200/"+str(model_name)+"/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
  junk=1

def log(error_type,error_result,error_msg):
  print error_type,error_result,error_msg,model_name
  try:
    junk=requests.post("http://localhost:9200/logs/_doc/",json={"timestamp":int(time.time())*1000,"status":error_result,"error_type":model_name+"_"+error_type,"error_msg":error_msg})
  except:
    junk=1

def get_params():
  r=requests.get("https://www.betfair.com/exchange/plus/")
  ak=r.text[r.text.index("appKey"):].split(":")[1].strip()[1:].split('"')[0].strip()
  return [ak]

[ak]=get_params()

def betfair_list_aus():
  global ak
  r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"maxResults":5,"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"eventTypeIds":[7],"selectBy":"RANK","marketTypeCodes":["WIN"],"attachments":["MARKET_LITE"],"marketStartingAfter":"2018-02-20T00:00:00.486Z"},"currencyCode":"USD","locale":"en"},headers={"Connection":"keep-alive","Accept":"application/json","Origin":"https://www.betfair.com","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 OPR/50.0.2762.67","content-type":"application/json","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.9"})
  r=requests.get("https://strands.betfair.com/api/eds/racing-navigation/v1?eventTypeId=7&navigationType=todayscard&raceId="+str(r.json()['attachments']['liteMarkets'][r.json()['attachments']['liteMarkets'].keys()[0]]['raceId']),headers={"X-Application":ak})
  race_list={}
  repeats=[]
  for t in r.json()['races']:
    if "AUS" in t['meetingName'] or "NZL" in t['meetingName']:
      eastern = pytz.timezone('Europe/London')
      pytz.utc = pytz.timezone('UTC')
      loc_dt = eastern.localize(dateutil.parser.parse(t['startTime']).replace(tzinfo=None))
      loc_dt = eastern.normalize(loc_dt)
      race_time = int(time.mktime(loc_dt.astimezone(pytz.utc).timetuple()))
      if race_time in race_list:
        repeats.append(race_time)
      else:
        race_list[race_time]=t['raceId']
  for t in repeats:
    del race_list[t]
  return race_list


def betfair_dat(raceId):
  global ak
  r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"exchangeIds":[1],"eventTypeIds":[7],"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"attachments":["MARKET_LITE"],"selectBy":"RANK","raceIds":[raceId]},"textQuery":None,"facets":[],"currencyCode":"USD","locale":"en"})
  dat={}
  rs=[grequests.get("https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak="+str(ak)+"&currencyCode=USD&locale=en&marketIds="+str(c)+"&rollupLimit=4&rollupModel=STAKE&types=MARKET_STATE,MARKET_DESCRIPTION,RUNNER_METADATA,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP") for c in [r.json()['attachments']['liteMarkets'][c]['marketId'] for c in r.json()['attachments']['liteMarkets']]]
  tmp1=grequests.map(rs)
  horse_map={}
  for r in tmp1:
    if r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['description']['marketType']=="WIN":
      horse_map={c['selectionId']:[c['description']['runnerName'],int(c['description']['metadata']['CLOTH_NUMBER'])] for c in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if 'CLOTH_NUMBER' in c['description']['metadata']}
  for r in tmp1:
    dat[r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['description']['marketType']]={"marketId":r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['marketId'],"marketData":sorted([{"horse_name":horse_map[c['selectionId']][0],"horse_number":int(horse_map[c['selectionId']][1]),"horse_name":c['description']['runnerName'],"midPrice":round((sorted(c['exchange']['availableToBack'],key=lambda x:x['price'])[-1]['price']+sorted(c['exchange']['availableToLay'],key=lambda x:x['price'])[0]['price'])/2.0,2),"backPrice":sorted(c['exchange']['availableToBack'],key=lambda x:x['price'])[-1]['price'],"layPrice":sorted(c['exchange']['availableToLay'],key=lambda x:x['price'])[0]['price']} for c in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['selectionId'] in horse_map and 'availableToLay' in c['exchange'] and 'availableToBack' in c['exchange']],key=lambda x:x['midPrice'])}
  r=requests.get("https://strands.betfair.com/api/eds/racing-info/v1?_ak="+str(ak)+"&marketId="+str(dat['WIN']['marketId']))
  fin_out={}
  for t2 in dat['WIN']['marketData']:
    hr_dat={"win_lay":None,"win_back":None,"place_lay":None,"place_back":None}
    hr_dat['horse_number']=t2['horse_number']
    hr_dat['horse_name']=t2['horse_name']
    fin_out[int(t2['horse_number'])]=hr_dat
  for t2 in dat['WIN']['marketData']:
    fin_out[t2['horse_number']]["win_lay"]=t2['layPrice']
    fin_out[t2['horse_number']]["win_back"]=t2['backPrice']
  if "PLACE" in dat:
    for t2 in dat['PLACE']['marketData']:
      if t2['horse_number'] in fin_out:
        fin_out[t2['horse_number']]["place_lay"]=t2['layPrice']
        fin_out[t2['horse_number']]["place_back"]=t2['backPrice']
  else:
    for t2 in dat['WIN']['marketData']:
      if t2['horse_number'] in fin_out:
        fin_out[t2['horse_number']]["place_lay"]=0.0
        fin_out[t2['horse_number']]["place_back"]=0.0    
  market_ids={}
  market_ids['WIN']=None
  market_ids['PLACE']=None
  for t in dat.keys():
    market_ids[t]=dat[t]['marketId']
  return [fin_out,market_ids]

def update_res():
  global model_name
  r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
  if 'hits' in r.json() and 'hits' in r.json()['hits']:
    for t in r.json()['hits']['hits']:
      tmp=t['_source']
      if "betfair_win_id" in tmp and tmp['betfair_win_id'] and "result_win" not in tmp:
        print tmp['betfair_win_id']
        params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['betfair_win_id'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
        headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
        r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
        winners=[c['description']['runnerName'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
        if winners:
          tmp['result_win']=winners[0]
      if "betfair_place_id" in tmp and tmp['betfair_place_id'] and "result_place" not in tmp:
        params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['betfair_place_id'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
        headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
        r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
        winners=[c['description']['runnerName'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
        if winners:
          tmp['result_place']=json.dumps(winners)
      junk=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(t['_id']),json=tmp)

def get_tf(market_id):
  global ak
  try:
    r=requests.get("https://apieds.betfair.com/api/eds/racing-info/v1?_ak="+str(ak)+"&marketId="+str(market_id))
    hr_map={r.json()['horseDetails'][c]['name']:r.json()['horseDetails'][c]['saddleCloth'] for c in r.json()['horseDetails']}
    return [hr_map[c['name']] for c in r.json()['timeform']['timeform123']]
  except:
    return None

try:
  aus_betfair_list = betfair_list_aus()
  log("init","Success","Skyracing:"+str(len(aus_betfair_list)))
except:
  log("init","Fail",str(traceback.format_exc()))


total_dat={}
while True:
  try:
    update_res()
  except:
    junk=1
  time.sleep(2)
  for t in sorted(aus_betfair_list):
    cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
    if cur_ts>=t-120 and cur_ts<=t+120 and t not in total_dat:
      try:
        dat1=betfair_dat(aus_betfair_list[t])
        final={}
        final['timestamp']=t*1000
        final['timeform']=get_tf(dat1[1]['WIN'])
        if final['timeform']:
          final['betfair_place_id']=dat1[1]['PLACE'] if "PLACE" in dat1[1] else None
          final['betfair_win_id']=dat1[1]['WIN'] if "WIN" in dat1[1] else None
          final['horse_data']=[]
          for t1 in dat1[0]:
            if dat1[0][t1]['win_back']:
              final['horse_data'].append(dat1[0][t1])
          final['horse_data']=sorted(final['horse_data'],key=lambda x:(x['win_back']))
          r=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(final['timestamp']),json=final)
          try:
            print r.text
          except:
            print "------"
          total_dat[t]="added"
          log("new_race","Success",None)
      except:
        log("new_race","Fail",str(traceback.format_exc()))




