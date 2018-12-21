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

def log(error_type,error_result,error_msg):
  try:
    junk=requests.post("http://localhost:9200/logs/_doc/",json={"timestamp":int(time.time())*1000,"status":error_result,"error_type":error_type,"error_msg":error_msg})
  except:
    junk=1

model_name="uk_hr_all_build11"

try:
  username="mohanbet365"
  password="mohankumar95"
  auth_session=requests.Session()
  r=auth_session.post("https://api.matchbook.com/bpapi/rest/security/session",json={"username":username,"password":password})
  log("matchbook_init","Success",None)
except:
  log("matchbook_init","Fail",str(traceback.format_exc()))

try:
 junk=requests.put("http://localhost:9200/"+str(model_name)+"/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
 junk=1

def matchbook_list():
 r=requests.get("https://api.matchbook.com/edge/rest/lookups/sports")
 horse_race_id=[c for c in r.json()['sports'] if c['name']=="Horse Racing"][0]['id']
 r=requests.get("https://api.matchbook.com/edge/rest/events?category-ids="+str(horse_race_id))
 out={}
 for t in r.json()['events']:
  if {c['type']:c['name'] for c in t['meta-tags']}['COUNTRY'].lower().strip() in ["uk","ireland"]:
   out[int(time.mktime(dateutil.parser.parse(t['start']).timetuple()))]={"race_name":t['name'],"id":t['id']}
 return out

def matchbook_place(eventId,horse_pred,stake=0.1,side="lay"):
  r=auth_session.get("https://api.matchbook.com/bpapi/rest/security/session")
  if r.status_code!=200:
    r=auth_session.post("https://api.matchbook.com/bpapi/rest/security/session",json={"username":username,"password":password}) 
  r=requests.get("https://www.matchbook.com/edge/rest/events/"+str(eventId)+"?language=en&currency=USD&exchange-type=back-lay&odds-type=DECIMAL&include-markets=true&include-prices=true&include-runners=true&market-per-page=500&price-depth=6&include-event-participants=true")
  out={}
  for market in r.json()['markets']:
   if market['name']=="WIN":
    for runner in market['runners']:
     horse_num=int(runner['name'].split(" ")[0].strip())
     horse_name=runner['name']
     try:
      out[horse_num]={"eventId":eventId,"runnerId":runner['id'],"odds":sorted([c for c in runner['prices'] if c['side']=="lay"],key=lambda x:x['decimal-odds'])[0]['decimal-odds']}
     except:
      junk=1
  if horse_pred in out:
    runnerId=out[horse_pred]['runnerId']
    odds=out[horse_pred]['odds']
    stake=stake if stake>=0.1 else 0.1
    r=auth_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":[{"runner-id":runnerId,"side":side,"odds":odds,"stake":stake}]})
    if "Insufficient funds" in str(r.text):
      print "Insufficient funds"
    elif 'id' in r.json()['offers'][0]:
      offer_id=r.json()['offers'][0]['id']
      print "Bet Placed..."
    else:
      print "Unknown Error..."
  else:
    print "Horse Pred not present"

def model1(list1,tmp,stake=0.1):
  try:
    for t1 in tmp['1_racinguk']:
      if t1['365dm_verdict'] and t1['odds']>5.0:
        hr_idx=sorted([c['tf_rating_int'] for c in tmp['1_racinguk']],reverse=True).index(t1['tf_rating_int'])
        filter1="365_rating" in t1 and t1['365_rating']<=1.0 and t1['365_rating']
        filter2=hr_idx>=4 and t1['tf_rating_int']
        odds=[c for c in tmp['betfair_win_dat'] if c['horse_number']==t1['horse_number']][0]['midPrice']
        if t1['horse_number'] in [c['horse_number'] for c in tmp['betfair_win_dat']] and (filter1 or filter2) and odds<20.0:
          matchbook_place(list1[tmp['timestamp']/1000]['id'],t1['horse_number'],stake)
  except:
    log("model_1","Fail",str(traceback.format_exc()))

total_dat=[]

while True:
  time.sleep(2)
  try:
    list1=matchbook_list()
  except:
    log("matchbook_list","Fail",str(traceback.format_exc()))
  try:
    r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "2model2_pred"}}}}})
    for t in r.json()['hits']['hits']:
      tmp=t['_source']
      cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
      if cur_ts<=tmp['timestamp']/1000:
        if tmp['timestamp']/1000 not in total_dat and tmp['timestamp']/1000 in list1:
          print "New Race......................."
          total_dat.append(tmp['timestamp']/1000)
          model1(list1,tmp,0.2)
  except:
    log("matchbook_loop","Fail",str(traceback.format_exc()))









