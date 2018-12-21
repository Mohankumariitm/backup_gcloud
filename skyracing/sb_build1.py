import time
import calendar
import requests
import grequests
from bs4 import BeautifulSoup
import thread
import datetime
import ast
import itertools
from pprint import pprint
import pickle
import pytz
import dateutil.parser
import traceback
from ngram import NGram

model_name="betfair_sportbook_uk_build1"
r_s=requests.session()
r=r_s.get("https://www.betfair.com/sport/tennis")
bffp="7489"
ak="nzIFcwyWhrlwYMrh"

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

def login():
  global r_s
  headerss={"Host":"identitysso.betfair.com","Connection":"keep-alive","Cache-Control":"max-age=0","Origin":"https://www.betfair.com","Upgrade-Insecure-Requests":"1","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36","Content-Type":"application/x-www-form-urlencoded","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8","Referer":"https://www.betfair.com/sport","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.8"}
  params={"product":"sportsbook","redirectMethod":"POST","url":"https://www.betfair.com/sport/login/success?rurl=https://www.betfair.com/sport","username":"mohankumarbet365@gmail.com","password":"mohankumar95"}
  r=r_s.post("https://identitysso.betfair.com/api/login",data=params,headers=headerss)

def update_bffp():
  global r_s,bffp
  r=r_s.get("https://www.betfair.com/sport/tennis")
  soup = BeautifulSoup(r.text, 'html.parser')
  t=soup.find_all(rel="shortcut icon")[0].get('href')
  bffp=t[t.index('favicon_')+8:t.index("_.ico")]

def get_balance():
  global r_s
  headerss={"Host":"www.betfair.com","Connection":"keep-alive","X-Application":"SharedSiteComponent","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36","X-BF-Jurisdiction":"international","Content-Type":"application/json","Accept":"*/*","Accept-Encoding":"gzip, deflate, sdch, br","Accept-Language":"en-US,en;q=0.8"}
  r=r_s.get("https://www.betfair.com/wallet-service/v3.0/wallets?walletNames=[MAIN,SPORTSBOOK_BONUS]&alt=json",headers=headerss)
  return r.json()[0]['details']['amount']

def get_xsrf():
  global r_s
  return r_s.cookies.get_dict()['xsrftoken']

def update_params():
  global ak
  r=requests.get("https://www.betfair.com/exchange/plus/")
  ak=r.text[r.text.index("appKey"):].split(":")[1].strip()[1:].split('"')[0].strip()

def get_score(eventId):
  r=r_s.get("https://ips.betfair.com/inplayservice/v1/scores?regionCode=UK&_ak="+str(ak)+"&alt=json&locale=en_GB&eventIds="+str(eventId)+"&ts="+str(calendar.timegm(time.gmtime()))+"&xsrftoken="+get_xsrf())
  cur_score={"current_set":r.json()[0]['currentSet'],"current_game":r.json()[0]['currentGame'],"away":int(r.json()[0]['score']['away']['games']),"home":int(r.json()[0]['score']['home']['games'])}
  return cur_score  

def get_data(cur_url):
  global r_s
  try:
    ids={c.split("=")[0]:c.split("=")[1] for c in cur_url.split("?")[1].split("&")}
    r=r_s.get("https://www.betfair.com/sport/horse-racing/meeting?exchangeWinMarketId="+str(ids["exchangeWinMarketId"])+"&eventId="+str(ids['eventId'])+"&marketType=MATCH_BET&raceTime="+str(ids['raceTime'])+"&action=loadRacecardTab&racecardTabType=MATCH_BET&dayToSearch="+str(ids['dayToSearch'])+"&modules=racecard@1090&d18=Main&d31=Middle&isAjax=true&ts="+str(calendar.timegm(time.gmtime()))+"&alt=json&xsrftoken="+get_xsrf())
    html_source=r.json()['page']['config']['instructions'][0]['arguments']['html']
    soup = BeautifulSoup(html_source, 'html.parser')
    cur_runner=soup.find_all(class_="match-bet-market")[0].find_all("a")[0]
    for c1 in r.json()['page']['config']['instructions'][4]['arguments']:
      if c1['marketId']==cur_runner.get('data-marketid'):
        odds_map={str(c['selectionId']):c['prices']['back'][0]['fraction'] for c in c1['runners']}
    markets=[]
    for t in soup.find_all(class_="match-bet-market"):
      try:
        cur_runner=t.find_all("a")[0]
        tmp={c.split("=")[0]:"=".join(c.split("=")[1:]) for c in cur_runner.get("href").split("?")[1].split("&")}
        tmp1={"horse_name":cur_runner.find_previous(class_="runner-name-value").text.strip(),"odds":float(cur_runner.text.replace("\n","").replace(" ","")),"den":odds_map[cur_runner.get('data-selectionid')]['denominator'],"num":odds_map[cur_runner.get('data-selectionid')]['numerator'],"bsmSt":tmp["bsmSt"],"gaZone":tmp['gaZone'],'price':cur_runner.text.replace("\n","").replace(" ",""),'eventId':cur_runner.get("data-eventid"),'bseId':cur_runner.get("data-eventid"),'bssId':cur_runner.get('data-selectionid'),'bsmId':cur_runner.get('data-marketid'),'bsUUID':cur_runner.get('data-uuid'),'bsGroup':cur_runner.get("data-eventid")}
        cur_runner=t.find_all("a")[1]
        tmp2={"horse_name":cur_runner.find_previous(class_="runner-name-value").text.strip(),"odds":float(cur_runner.text.replace("\n","").replace(" ","")),"den":odds_map[cur_runner.get('data-selectionid')]['denominator'],"num":odds_map[cur_runner.get('data-selectionid')]['numerator'],"bsmSt":tmp["bsmSt"],"gaZone":tmp['gaZone'],'price':cur_runner.text.replace("\n","").replace(" ",""),'eventId':cur_runner.get("data-eventid"),'bseId':cur_runner.get("data-eventid"),'bssId':cur_runner.get('data-selectionid'),'bsmId':cur_runner.get('data-marketid'),'bsUUID':cur_runner.get('data-uuid'),'bsGroup':cur_runner.get("data-eventid")}
        markets.append([tmp1,tmp2] )  
      except:
        junk=1
    return markets
  except:
    return None

def place_bet(player,bet_amt="0.13"):
  global r_s
  headerss={"Host":"www.betfair.com","Connection":"keep-alive","X-Requested-With":"XMLHttpRequest","BF-FP":bffp,"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36","Accept":"*/*","Accept-Encoding":"gzip, deflate, sdch, br","Accept-Language":"en-US,en;q=0.8"}
  r=r_s.get("https://www.betfair.com/sport/horse-racing?gaZone=Main&bseId="+player['bseId']+"&bsContext=REAL&bsmSt="+player['bsmSt']+"&bsUUID="+player['bsUUID']+"&gaMod=multipickavb&eventId="+player['eventId']+"&bssId="+player['bssId']+"&action=addSelection&bsmId="+player['bsmId']+"&modules=betslip&isSP=false&xsrftoken="+get_xsrf()+"&bsGroup="+player['bsGroup']+"&ts="+str(calendar.timegm(time.gmtime()))+"&alt=json",headers=headerss)
  bet_id=str(r.json()['page']['config']['instructions'][1]['arguments']['betRowId'].replace("#",""))
  html_source=r.json()['page']['config']['instructions'][1]['arguments']['event']
  soup = BeautifulSoup(html_source, 'html.parser') 
  max_val=str(soup.find_all(id=bet_id+"-maxStake")[0].get("value"))
  min_val=str(soup.find_all(id=bet_id+"-minStake")[0].get("value"))
  headerss={"Host":"www.betfair.com","Connection":"keep-alive","Content-Length":"917","Origin":"https://www.betfair.com","X-Requested-With":"XMLHttpRequest","BF-FP":bffp,"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36","Content-Type":"application/x-www-form-urlencoded","Accept":"*/*","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.8"}
  params={"timestamp":str(calendar.timegm(time.gmtime())),bet_id+"-inputStake":str(bet_amt),bet_id+"-stake":str(bet_amt),bet_id+"-odd":str(player['price']),bet_id+"-maxStake":max_val,bet_id+"-minStake":min_val,bet_id+"-tfoNumerator":str(player['num']),bet_id+"-tfoDenominator":str(player['den']),bet_id+"-dfoNumerator":str(player['num']),bet_id+"-dfoDenominator":str(player['den']),bet_id+"-bsIds":str(player['bsmId']).replace(".","_")+"-"+str(player['bssId']),bet_id+"-suspended":"false",bet_id+"-suspendedRunner":"false",bet_id+"-sequence":"0"}
  r=r_s.post("https://www.betfair.com/sport/place-bet?redirectPath=/football&action=place&modules=betslip&bsContext=REAL&xsrftoken="+get_xsrf(),data=params,headers=headerss)
  return [max_val,min_val]

def get_list():
  rlist={}
  for i in range(2):
    r=requests.get("https://www.betfair.com/sport/horse-racing?action=loadTab&tab=TODAY&dayToSearch="+str((datetime.datetime.now()+datetime.timedelta(days=i)).strftime("%Y%m%d"))+"&modules=multipick-horse-racing")
    soup = BeautifulSoup(r.text, 'html.parser')
    for t in soup.find_all(class_="meeting-item"):
      if t.get("data-country-name") in ['gb','ie']:
        for t1 in t.find_all(class_="outbound-link"):
          cur_url="https://www.betfair.com"+t1.get("href")
          tmp={c.split("=")[0]:c.split("=")[1] for c in cur_url.split("?")[1].split("&")}
          if "raceTime" in tmp:
            rlist[int(tmp["raceTime"])/1000]=cur_url
  return rlist

def update_result():
  global model_name
  r=requests.get("http://localhost:9200/"+str(model_name)+"/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
  if 'hits' in r.json() and 'hits' in r.json()['hits']:
    for t1 in r.json()['hits']['hits']:
      tmp=t1['_source']
      #if "model_match" in tmp and tmp['model_match']:
      if ("result" not in tmp or not tmp['result']) and "model_match" in tmp and tmp['model_match']:
        try:
          hrs=sorted(tmp['model_match'],key=lambda x:x['odds'])
          tmp['result']=None
          if "horse_name" in hrs[0]:
            r1=requests.get(tmp['race_url']+"&history=true")
            soup=BeautifulSoup(r1.text,"html.parser")
            flag=0
            for t in soup.find_all(class_="result"):
              if NGram.compare(t.find(class_="horse").text.split("(")[0].strip().lower(),hrs[0]['horse_name'].lower().strip())>=0.75 or NGram.compare(t.find(class_="horse").text.strip().lower(),hrs[1]['horse_name'].lower().strip())>=0.75:
                flag=1
            if flag:
              tmp['result']="void"
              for t in soup.find_all(class_="result"):
                if t.find(class_="pos").text.strip().isdigit() and NGram.compare(t.find(class_="horse").text.split("(")[0].strip().lower(),hrs[0]['horse_name'].lower().strip())>=0.75:
                  tmp['result']="low"
                  break
                elif t.find(class_="pos").text.strip().isdigit() and NGram.compare(t.find(class_="horse").text.split("(")[0].strip().lower(),hrs[1]['horse_name'].lower().strip())>=0.75:
                  tmp['result']="high"
                  break
          print tmp['result']
          junk=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(t1['_id']),json=tmp)          
        except:
          print str(traceback.format_exc())

def model(dat):
  stake=0.13
  highs=[]
  for t in dat:
    highs.append([sorted(t,key=lambda x:x['odds'])[0],t])
  model_1=None
  min_val=None
  max_val=None
  for t in sorted(highs,key=lambda x:x[0]['odds']):
    if t[0]['odds']>=1.5 and t[0]['odds']<=1.8:
      model_1=t[1]
      [max_val,min_val]=place_bet(t[0],str(stake))
      break
  return [model_1,max_val,min_val,stake]

update_bffp()
update_params()
login()

race_list=get_list()
log("init","Success",None)

total_dat={}
while True:
  try:
    update_result()
  except:
    junk=1
  time.sleep(2)
  for t in sorted(race_list):
    cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
    if (cur_ts-t)>=-60 and (cur_ts-t)<=60 and t not in total_dat:
      try:
        dat=get_data(race_list[t])
        if dat:
          try:
            get_balance()
          except:
            login()
          tmp_dat=model(dat)
          final={}
          final['timestamp'] = t*1000
          final['race_url'] = race_list[t]
          final['model_match']=tmp_dat[0]
          final['model_max']=tmp_dat[1]
          final['model_min']=tmp_dat[2]
          final['model_stake']=tmp_dat[3]
          for j in range(len(dat)):
            final['match_'+str(j+1)]=dat[j]
          r=requests.post("http://localhost:9200/"+str(model_name)+"/_doc/"+str(final['timestamp']),json=final)
          print r.text
          total_dat[t]="added"
          log("new_race","Success",None)
      except:
        log("new_race","Fail",str(traceback.format_exc()))




