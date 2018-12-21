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
from pprint import pprint
import traceback

stake=0.15
side="lay"
odds=100.0
matchbook_thresh=100.0
sp_thresh=25.0

pr_name="handicap_blind"
model_name="handicap_blind"

matchbook_username="mohanbet365"
matchbook_password="mohankumar95"

matchbook_session=requests.Session()

def log(error_type,error_result,error_msg):
  global pr_name
  print error_type,error_result,error_msg
  try:
    junk=requests.post("http://localhost:9200/logs/_doc/",json={"timestamp":int(time.time())*1000,"status":error_result,"error_type":str(pr_name)+"_"+str(error_type),"error_msg":error_msg})
  except:
    junk=1

try:
  junk=requests.put("http://localhost:9200/logs/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
  junk=requests.put("http://localhost:9200/"+str(model_name)+"/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
  junk=1

def attheraces_list():
  r=requests.get("http://www.attheraces.com/racecards")
  soup=BeautifulSoup(r.text,"html.parser")
  out={}
  for t1 in soup.find_all(class_="panel uk")+soup.find_all(class_="panel eire"):
    for t2 in t1.find_all(class_="meeting"):
      try:
        race_url="http://www.attheraces.com"+t2.get("href")
        race_time=int(time.mktime(dateutil.parser.parse(t2.find("meta",{"itemprop":"startDate"}).get("content").strip()+".000Z").timetuple()))
        out[race_time]=race_url
      except:
        junk=1
  return out

def attheraces_type(cur_url):
  global sp_thresh
  r=requests.get(cur_url)
  soup=BeautifulSoup(r.text,"html.parser")
  race_description=soup.find(class_="race-head__main").find("strong").text.lower()
  hr=[]
  if "handicap" in race_description:
    for t in soup.find(class_="premium-odds-partners").find_all("tr",{"class":"decimal"}):
      if float(t.find(class_="odds").text.strip())>=sp_thresh:
        hr.append(int(t.find(class_="number").text.strip()))
  return hr

def matchbook_login():
  global matchbook_session,matchbook_username,matchbook_password
  for i in range(5):
    try:
      r=matchbook_session.get("https://api.matchbook.com/bpapi/rest/security/session")
      if r.status_code!=200:
        r=matchbook_session.post("https://api.matchbook.com/bpapi/rest/security/session",json={"username":matchbook_username,"password":matchbook_password})
      return 0
    except:
      junk=1
  log("login","Fail",str(traceback.format_exc()))

def matchbook_map(cur_race_time):
  for i in range(5):
    try:
      r=requests.get("https://api.matchbook.com/edge/rest/lookups/sports")
      horse_race_id=[c for c in r.json()['sports'] if c['name']=="Horse Racing"][0]['id']
      r=requests.get("https://api.matchbook.com/edge/rest/events?category-ids="+str(horse_race_id))
      out={}
      for t in r.json()['events']:
        if {c['type']:c['name'] for c in t['meta-tags']}['COUNTRY'].lower().strip() in ["uk","ireland"]:
          out[int(time.mktime(dateutil.parser.parse(t['start']).timetuple()))]={"race_name":t['name'],"id":t['id']}
      eventId=out[int(cur_race_time)]['id']
      r=requests.get("https://www.matchbook.com/edge/rest/events/"+str(eventId)+"?language=en&currency=USD&exchange-type=back-lay&odds-type=DECIMAL&include-markets=true&include-prices=true&include-runners=true&market-per-page=500&price-depth=6&include-event-participants=true")
      out=[]
      for market in r.json()['markets']:
        if market['name']=="WIN":
          for runner in market['runners']:
            horse_num=int(runner['name'].split(" ")[0].strip())
            horse_name=runner['name']
            try:
              out.append({"horse_number":horse_num,"horse_name":horse_name,"eventId":eventId,"runnerId":runner['id'],"odds":sorted([c for c in runner['prices'] if c['side']=="lay"],key=lambda x:x['decimal-odds'])[0]['decimal-odds']})
            except:
              junk=1
      return out
    except:
      junk=1
  return 0


try:
  list1=attheraces_list()
  log(model_name+"_init","Success","Total Races:"+str(len(list1)))  
except:
  log(model_name+"_init","Fail",str(traceback.format_exc()))


matchbook_login()

total_dat=[]
while True:
  time.sleep(2)
  for t in sorted(list1):
    cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
    if cur_ts>=t-180 and cur_ts<=t+120 and t not in total_dat:
      print "checking race..."
      try:
        last_hr=attheraces_type(list1[t])
        if last_hr:
          print "Available horses",str(last_hr),"..."
          total_dat.append(t)
          matchbook_login()
          hr_list=sorted(matchbook_map(t),key=lambda x:x['odds'],reverse=True)
          for t in hr_list:
            if t['odds']<=matchbook_thresh and t['odds']>=sp_thresh+5.0 and t['horse_number'] in last_hr:
              r=matchbook_session.post("https://api.matchbook.com/edge/rest/offers",json={"odds-type":"DECIMAL","exchange-type":"back-lay","offers":[{"runner-id":t['runnerId'],"side":side,"odds":odds,"stake":stake}]})
              race_log={}
              race_log['runners']=hr_list
              race_log['response_matchbook']=r.text
              race_log['horse_list_eligible']=last_hr
              race_log['timestamp']=int(time.time())*1000
              junk=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/",json=race_log)
              log(model_name+"_placed","Success",r.text)
              break
        else:
          total_dat.append(t)
      except:
        log(model_name+"_new_race","Fail",str(traceback.format_exc()))










