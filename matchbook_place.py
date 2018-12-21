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

model_1_max_thresh=15.0
lay_odds=15.0
lay_stake=0.2

model_name="uk_hr_all_build12"
matchbook_username="mohanbet365"
matchbook_password="mohankumar95"
matchbook_session=requests.Session()

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

def model_1(tmp):
  global model_1_max_thresh,lay_odds,lay_stake
  total_tips=sum([c['rp_tips'] for c in tmp['1_racinguk']])
  odds_map={c['horse_number']:c['midPrice'] for c in tmp['betfair_win_dat']}
  horses=[c for c in tmp['1_racinguk'] if c['horse_number'] in odds_map and not c['rp_tips']]
  hr=sorted(horses,key=lambda x:odds_map[x['horse_number']])
  if hr and odds_map[hr[0]['horse_number']]<=model_1_max_thresh:
    pred=hr[0]['horse_number']
    matchbook_horse_map=matchbook_map(int(tmp['timestamp']/1000))
    offers=[{"runner-id":matchbook_horse_map[pred]['runnerId'],"side":"lay","odds":lay_odds,"stake":lay_stake}]
    r=matchbook_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":offers})
    log("matchbook_bet_placed","success",r.text)

horse_race_id=matchbook_params()
matchbook_login()
log("matchbook_init","Success",None)

total_dat=[]
while True:
  time.sleep(2)
  try:
    r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "2model2_pred"}}}}})
    for t in r.json()['hits']['hits']:
      tmp=t['_source']
      cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
      if cur_ts<=tmp['timestamp']/1000 and tmp['timestamp']/1000 not in total_dat:
        print "New Race......................."
        matchbook_login()
        model_1(tmp)
        total_dat.append(tmp['timestamp']/1000)
  except:
    log("matchbook_loop","Fail",str(traceback.format_exc()))









