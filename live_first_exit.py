import json
import requests
import datetime
import time
import dateutil.parser
from datetime import timedelta
import sys, os
from pprint import pprint
import traceback
import threading

live_thresh=25.0
sp_thresh=12.5
loop_speed_thresh=100.0
back_stake=5.0
back_odds=1.01
lay_stake=0.11
lay_odds=100.0

try:
  junk=requests.put("http://localhost:9200/logs/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
  junk=1

def log(error_type,error_result,error_msg):
  print error_type,error_result,error_msg
  try:
    junk=requests.post("http://localhost:9200/logs/_doc/",json={"timestamp":int(time.time())*1000,"status":error_result,"error_type":error_type,"error_msg":error_msg})
  except:
    junk=1

matchbook_username="mohanbet365"
matchbook_password="mohankumar95"
betfair_username="mohankumarbet365@gmail.com"
betfair_password="mohankumar95"

matchbook_session=requests.Session()
betfair_session=requests.Session()

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

def get_params():
  r=requests.get("https://www.betfair.com/exchange/plus/")
  ak=r.text[r.text.index("appKey"):].split(":")[1].strip()[1:].split('"')[0].strip()
  return ak

def matchbook_params():
  r=requests.get("https://api.matchbook.com/edge/rest/lookups/sports")
  return [c for c in r.json()['sports'] if c['name']=="Horse Racing"][0]['id']

def betfair_login():
  global betfair_session,betfair_username,betfair_password
  headerss={"Host":"identitysso.betfair.com","Connection":"keep-alive","Cache-Control":"max-age=0","Origin":"https://www.betfair.com","Upgrade-Insecure-Requests":"1","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36","Content-Type":"application/x-www-form-urlencoded","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8","Referer":"https://www.betfair.com/sport","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.8"}
  params={"product":"sportsbook","redirectMethod":"POST","url":"https://www.betfair.com/sport/login/success?rurl=https://www.betfair.com/sport","username":betfair_username,"password":betfair_password}
  r=betfair_session.post("https://identitysso.betfair.com/api/login",data=params,headers=headerss)

def betfair_list():
  global ak
  r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"maxResults":5,"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"eventTypeIds":[7],"selectBy":"RANK","marketTypeCodes":["WIN"],"attachments":["MARKET_LITE"],"marketStartingAfter":"2018-02-20T00:00:00.486Z"},"currencyCode":"USD","locale":"en"},headers={"Connection":"keep-alive","Accept":"application/json","Origin":"https://www.betfair.com","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 OPR/50.0.2762.67","content-type":"application/json","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.9"})
  r=requests.get("https://strands.betfair.com/api/eds/racing-navigation/v1?eventTypeId=7&navigationType=todayscard&raceId="+str(r.json()['attachments']['liteMarkets'][r.json()['attachments']['liteMarkets'].keys()[0]]['raceId']),headers={"X-Application":ak})
  race_list={}
  repeats=[]
  for t in r.json()['races']:
    if "(" not in t['meetingName']:
      if int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple())) in race_list:
        repeats.append(int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple())))
      else:
        race_list[int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple()))]=t['winMarketId']
  for t in repeats:
    try:
      del race_list[t]
    except:
      junk=1
  fin_out={}
  for t in race_list:
    if datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d')==datetime.datetime.fromtimestamp(sorted(race_list)[0]).strftime('%Y-%m-%d'):
      fin_out[t]=race_list[t]
  return fin_out

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
      out={}
      for market in r.json()['markets']:
        if market['name']=="WIN":
          for runner in market['runners']:
            horse_num=int(runner['name'].split(" ")[0].strip())
            out[horse_num]={"eventId":eventId,"runnerId":runner['id']}
      return out
    except:
      junk=1
  return 0

def slave_worker(cur_race_time):
  global loop_speed_thresh,horse_race_id,matchbook_session,betfair_session,list1,sp_thresh,live_thresh,back_stake,back_odds,lay_stake,lay_odds
  win_marketid=list1[cur_race_time]
  start_time=int(time.time())
  flag=1
  while (int(time.time())-start_time)<900 and flag:
    try:
      race_start_time=int(time.time())
      matchbook_login()
      matchbook_horse_map=matchbook_map(cur_race_time)
      while (int(time.time())-race_start_time)<600 and flag:
        try:
          r=requests.get("https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak="+str(ak)+"&currencyCode=USD&locale=en&marketIds="+str(win_marketid)+"&rollupLimit=4&rollupModel=STAKE&types=MARKET_STATE,MARKET_DESCRIPTION,RUNNER_METADATA,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP")
          betfair_horse_map={c['selectionId']:{"horse_number":int(c['description']['metadata']['CLOTH_NUMBER']),"sp":round(float(c['sp']['actualStartingPrice']),2)} for c in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if 'description' in c and 'metadata' in c['description'] and 'CLOTH_NUMBER' in c['description']['metadata']}
          if betfair_horse_map:
            print "horsemap done....."
            while (int(time.time())-race_start_time)<600 and flag:
              try:
                loop_time=time.time()
                r=betfair_session.get("https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak="+str(ak)+"&currencyCode=USD&locale=en&marketIds="+str(win_marketid)+"&rollupLimit=4&rollupModel=STAKE&types=RUNNER_EXCHANGE_PRICES_BEST")
                cur_hr=sorted([[t1['exchange']['availableToBack'][0]['price'],t1['selectionId']] for t1 in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if "exchange" in t1 and "availableToBack" in t1['exchange'] and t1['exchange']['availableToBack'][0]['price']>=live_thresh])
                if cur_hr:
                  if cur_hr[0][1] in betfair_horse_map:
                    if betfair_horse_map[cur_hr[0][1]]['sp']<=sp_thresh:
                      pred=betfair_horse_map[cur_hr[0][1]]['horse_number']
                      if pred in matchbook_horse_map:
                        offers=[{"runner-id":matchbook_horse_map[pred]['runnerId'],"side":"lay","odds":lay_odds,"stake":lay_stake}]
                        r=matchbook_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":offers})
                        log("bet_placed","success",str(cur_hr)+str(r.text))
                      else:
                        junk=1
                      flag=0
                  elif cur_hr[0][0]>=loop_speed_thresh:
                    time.sleep(max([0.5-(time.time()-loop_time),0.0]))
                  else:
                    time.sleep(max([0.1-(time.time()-loop_time),0.0]))
                else:
                  time.sleep(1)
              except:
                log("slave_worker_loop3","Fail",str(traceback.format_exc()))
                time.sleep(1)#
          time.sleep(1)
        except:
          time.sleep(1)#
    except:
      log("slave_worker_loop1","Fail",str(traceback.format_exc()))
      time.sleep(1)#

try:
  horse_race_id=matchbook_params()
  ak=get_params()
  list1=betfair_list()
  matchbook_login()
  betfair_login()
  log("live_worker","success",None)
except:
  log("live_worker","Fail",str(traceback.format_exc()))

thread_pool=[]
placed=[]
while True:
  try:
    time.sleep(2)
    for t in sorted(list1):
      cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
      if cur_ts>=t-180 and cur_ts<=t+120 and t not in placed:
        placed.append(t)
        junk=threading.Thread(target=slave_worker, args=(t,))
        junk.start()
        thread_pool.append(junk)
        log("new_thread","Success",str(t))
  except:
    log("main_loop","Fail",str(traceback.format_exc()))
    time.sleep(1)



















