import json
import requests
import datetime
import time
import pickle
import base64
from bs4 import BeautifulSoup
import dateutil.parser
from datetime import timedelta
import sys, os
import calendar
import grequests
from pprint import pprint
import traceback
from websocket import create_connection
import pytz

min_stake=0.1
model_name="matchbook_exchange_double"
matchbook_username="mohanbet365"
matchbook_password="mohankumar95"
matchbook_session=requests.Session()

try:
  junk=requests.put("http://localhost:9200/logs/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
  junk=1

try:
  junk=requests.put("http://localhost:9200/"+str(model_name)+"/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
  junk=1

def log(error_type,error_result,error_msg):
  print error_type,error_result,error_msg
  try:
    junk=requests.post("http://localhost:9200/logs/_doc/",json={"timestamp":int(time.time())*1000,"status":error_result,"error_type":error_type,"error_msg":error_msg})
  except:
    junk=1

def get_params():
  r=requests.get("https://www.betfair.com/exchange/plus/")
  ak=r.text[r.text.index("appKey"):].split(":")[1].strip()[1:].split('"')[0].strip()
  return [ak]

[ak]=get_params()

def matchbook_login():
  for i in range(5):
    try:
      global matchbook_session,matchbook_username,matchbook_password
      r=matchbook_session.get("https://api.matchbook.com/bpapi/rest/security/session")
      if r.status_code!=200:
        r=matchbook_session.post("https://api.matchbook.com/bpapi/rest/security/session",json={"username":matchbook_username,"password":matchbook_password})
      return 0
    except:
      junk=1
  log("matchbook_login","Fail",str(traceback.format_exc()))

def matchbook_params():
  r=requests.get("https://api.matchbook.com/edge/rest/lookups/sports")
  return [c for c in r.json()['sports'] if c['name']=="Horse Racing"][0]['id']

def matchbook_map(cur_race_time):
  global horse_race_id
  for i in range(5):
    try:
      r=requests.get("https://api.matchbook.com/edge/rest/events?category-ids="+str(horse_race_id))
      out={}
      for t in r.json()['events']:
        if {c['type']:c['name'] for c in t['meta-tags']}['COUNTRY'].lower().strip() in ["uk","ireland"]:
          out[int(time.mktime(dateutil.parser.parse(t['start']).timetuple()))]={"race_name":t['name'],"id":t['id']}
      eventId=out[cur_race_time]['id']
      r=requests.get("https://www.matchbook.com/edge/rest/events/"+str(eventId)+"?language=en&currency=USD&exchange-type=back-lay&odds-type=DECIMAL&include-markets=true&include-prices=true&include-runners=true&market-per-page=500&price-depth=6&include-event-participants=true")
      out=[]
      for market in r.json()['markets']:
        if market['name']=="WIN":
          for runner in market['runners']:
            horse_num=int(runner['name'].split(" ")[0].strip())
            lay_prices=[c['decimal-odds'] for c in sorted(runner['prices'],key=lambda x:x['decimal-odds']) if c['side']=="lay"]
            if lay_prices:
              out.append({"odds":lay_prices[0],"horse_number":int(runner['name'].split(" ")[0]),"horse_name":runner['name'],"eventId":eventId,"runnerId":runner['id']})
      return out
    except:
      junk=1
  return 0

def betfair_list_uk():
  global ak
  r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"maxResults":5,"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"eventTypeIds":[7],"selectBy":"RANK","marketTypeCodes":["WIN"],"attachments":["MARKET_LITE"],"marketStartingAfter":"2018-02-20T00:00:00.486Z"},"currencyCode":"USD","locale":"en"},headers={"Connection":"keep-alive","Accept":"application/json","Origin":"https://www.betfair.com","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 OPR/50.0.2762.67","content-type":"application/json","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.9"})
  r=requests.get("https://strands.betfair.com/api/eds/racing-navigation/v1?eventTypeId=7&navigationType=todayscard&raceId="+str(r.json()['attachments']['liteMarkets'][r.json()['attachments']['liteMarkets'].keys()[0]]['raceId']),headers={"X-Application":ak})
  market_ids=[]
  id_map={}
  for t in r.json()['races']:
    market_ids.append(t['winMarketId'])
    id_map[t['winMarketId']]=t['raceId']
  rs=[grequests.get("https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak=nzIFcwyWhrlwYMrh&currencyCode=USD&locale=en&marketIds="+cur_market_id+"&rollupLimit=4&rollupModel=STAKE&types=EVENT,MARKET_DESCRIPTION") for cur_market_id in market_ids]
  tmp=grequests.map(rs)
  race_list={}
  for r in tmp:
    if r.json()['eventTypes'][0]['eventNodes'][0]['event']['countryCode'] in ['GB','IE']:
      eastern = pytz.timezone('Europe/London')
      pytz.utc = pytz.timezone('UTC')
      loc_dt = eastern.localize(dateutil.parser.parse(r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['description']['marketTime']).replace(tzinfo=None))
      loc_dt = eastern.normalize(loc_dt)
      race_time = int(time.mktime(loc_dt.astimezone(pytz.utc).timetuple()))
      race_list[race_time]={"race_id":id_map[r.url.split("&")[3].split("=")[1]],"win_market_id":r.url.split("&")[3].split("=")[1]}
  out={}
  prev=0
  for t in sorted(race_list):
    if prev and abs(prev-t)<=18000:
      out[t]=race_list[t]
      prev=t
    elif not prev:
      out[t]=race_list[t]
      prev=t      
    else:
      break
  return out

def get_status(cur_race_id):
  global ak
  try:
    socketURL="https://wss.betfair.com/fanout/?x-application="+str(ak)+"&EIO=3&transport=websocket"
    proto = socketURL.split('://')[0]
    server = socketURL.split('://')[1].split("/")[0]
    server_dir = socketURL.replace(proto + '://' + server, "")
    if proto == "http":
      socketURL = "ws://" + server + ":80" + server_dir
    elif proto == "https":
      socketURL = "wss://" + server + ":443" + server_dir
    ws = create_connection(socketURL)
    ws.settimeout(1.0)
    tmp=ws.recv()
    tmp=ws.recv()
    ws.send('40/raceStatus')
    tmp=ws.recv()
    ws.send('42/raceStatus,["sub","'+str(cur_race_id)+'"]')
    dat=ws.recv()
    status=['dormant','goingdown','atthepost','goingbehind','off','result']
    return status.index(json.loads(dat[dat.index(",")+1:])[1]['data'])+1
  except:
    return 0

def betfair_hrmap(market_id):
  params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(market_id)),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_METADATA,RUNNER_SP'),)
  headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
  r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
  res={}
  for t2 in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners']:
    res[int(t2['description']['metadata']['CLOTH_NUMBER'])]=t2['selectionId']
  return res

def place_bet(runner_id,stake,odds):
  global matchbook_session
  offers=[{"runner-id":runner_id,"side":"lay","odds":odds,"stake":stake}]
  r=matchbook_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":offers})
  log("bet_placed","success",r.text)

def update_result():
  global model_name
  r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
  if 'hits' in r.json() and 'hits' in r.json()['hits']:
    for t in r.json()['hits']['hits']:
      tmp=t['_source']
      if "betfair_win_market_id" in tmp and tmp['betfair_win_market_id'] and "result_win" not in tmp:
        params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['betfair_win_market_id'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
        headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
        r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
        winners=[c['selectionId'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
        if winners:
          print "Result Updated"
          tmp['result_win']=winners[0]
          tmp['result_flag']=True if str(winners[0])!=str(tmp['model_pred_betfair_id']) else False
      junk=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(t['_id']),json=tmp)


horse_race_id=matchbook_params()
matchbook_login()
betfair_list=betfair_list_uk()
log(model_name+"_init","Success",None)



total_dat=[]
while True:
  time.sleep(2)
  update_result()
  if(1):
  #try:
    for t in sorted(betfair_list):
      cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
      r2=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
      stake=round(min_stake,2)
      flag=False
      if r2.json()['hits']['hits']:
        hr=sorted(r2.json()['hits']['hits'],key=lambda x:x['_id'],reverse=True)[0]
        if "result_flag" in hr['_source']:
          flag=True
      else:
        flag=True
      if flag:  
        if abs(cur_ts-t)<=600 and t not in total_dat:
          race_status=get_status(betfair_list[t]['race_id'])
          print race_status
          if race_status==3:
            hr_dat=matchbook_map(t)
            hr_map=betfair_hrmap(betfair_list[t]['win_market_id'])
            final={}
            final['timestamp']=t*1000
            final['betfair_win_market_id']=betfair_list[t]['win_market_id']
            final['betfair_race_id']=betfair_list[t]['race_id']
            final['horse_data']=[]
            for hr in hr_dat:
              if hr['horse_number'] in hr_map:
                hr['betfair_id']=hr_map[hr['horse_number']]
                final['horse_data'].append(hr)
            model_pred=None
            for hr in sorted(final['horse_data'],key=lambda x:x['odds']):
              if hr['odds']>=1.0 and hr['odds']<=5.0:
                model_pred=hr
                break
            if model_pred:
              r2=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
              stake=round(min_stake,2)
              if r2.json()['hits']['hits']:
                hr=sorted(r2.json()['hits']['hits'],key=lambda x:x['_id'],reverse=True)[0]
                if hr['_source']["result_flag"]:
                  print "result_flag",hr['_source']["result_flag"]
                  stake=round(min_stake,2)
                else:
                  stake=round(float(model_pred['odds'])*hr['_source']['model_pred_stake'],2)
              #matchbook_login()
              #place_bet(model_pred['runnerId'],stake,30.0)
              final['model_pred_horse_number']=str(model_pred['horse_number'])
              final['model_pred_betfair_id']=str(model_pred['betfair_id'])
              final['model_pred_odds']=model_pred['odds']
              final['model_pred_stake']=stake
              r=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(final['timestamp']),json=final)
              print "New Race Added..."
              total_dat.append(t)







