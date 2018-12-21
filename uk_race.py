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

def get_params():
 r=requests.get("https://www.betfair.com/exchange/plus/")
 ak=r.text[r.text.index("appKey"):].split(":")[1].strip()[1:].split('"')[0].strip()
 return [ak]

[ak]=get_params()

try:
 junk=requests.put("http://localhost:9200/uk_hr_all_build5/",json={"mappings": {"_doc": {"properties": {"timestamp": {"type": "date" }}}}})
except:
 junk=1

def betfair_list(event_type_id=7):
 global ak
 r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"maxResults":5,"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"eventTypeIds":[event_type_id],"selectBy":"RANK","marketTypeCodes":["WIN"],"attachments":["MARKET_LITE"],"marketStartingAfter":"2018-02-20T00:00:00.486Z"},"currencyCode":"USD","locale":"en"},headers={"Connection":"keep-alive","Accept":"application/json","Origin":"https://www.betfair.com","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 OPR/50.0.2762.67","content-type":"application/json","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.9"})
 r=requests.get("https://strands.betfair.com/api/eds/racing-navigation/v1?eventTypeId="+str(event_type_id)+"&navigationType=todayscard&raceId="+str(r.json()['attachments']['liteMarkets'][r.json()['attachments']['liteMarkets'].keys()[0]]['raceId']),headers={"X-Application":ak})
 race_list={}
 for t in r.json()['races']:
  if int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple())) in race_list and "(" not in t['meetingName']:
   del race_list[int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple()))] 
  else:
   if "(" not in t['meetingName'] and int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple())) not in race_list:
    race_list[int(time.mktime(dateutil.parser.parse(t['startTime']).timetuple()))]=t['raceId']
 return race_list

def betfair_dat(raceId):
 r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"exchangeIds":[1],"eventTypeIds":[7],"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"attachments":["MARKET_LITE"],"selectBy":"RANK","raceIds":[raceId]},"textQuery":None,"facets":[],"currencyCode":"USD","locale":"en"})
 dat={}
 rs=[grequests.get("https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak="+str(ak)+"&currencyCode=USD&locale=en&marketIds="+str(c)+"&rollupLimit=4&rollupModel=STAKE&types=MARKET_STATE,MARKET_DESCRIPTION,RUNNER_METADATA,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP") for c in [r.json()['attachments']['liteMarkets'][c]['marketId'] for c in r.json()['attachments']['liteMarkets']]]
 tmp1=grequests.map(rs)
 horse_map={}
 for r in tmp1:
  if r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['description']['marketType']=="WIN":
   horse_map={c['selectionId']:int(c['description']['metadata']['CLOTH_NUMBER']) for c in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if 'CLOTH_NUMBER' in c['description']['metadata']}
 for r in tmp1:
  dat[r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['description']['marketType']]={"marketId":r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['marketId'],"marketData":sorted([{"horse_number":int(horse_map[c['selectionId']]),"horse_name":c['description']['runnerName'],"midPrice":round((sorted(c['exchange']['availableToBack'],key=lambda x:x['price'])[-1]['price']+sorted(c['exchange']['availableToLay'],key=lambda x:x['price'])[0]['price'])/2.0,2),"backPrice":sorted(c['exchange']['availableToBack'],key=lambda x:x['price'])[-1]['price'],"layPrice":sorted(c['exchange']['availableToLay'],key=lambda x:x['price'])[0]['price']} for c in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['selectionId'] in horse_map and 'availableToLay' in c['exchange'] and 'availableToBack' in c['exchange']],key=lambda x:x['midPrice'])}
 out={}
 if "WIN" in dat:
  out['betfair_win']=dat['WIN']['marketData']
  out['betfair_win_marketId']=dat['WIN']['marketId']
 else:
  out['betfair_win']=None
  out['betfair_win_marketId']=None
 if "PLACE" in dat:
  out['betfair_place']=dat['PLACE']['marketData']
  out['betfair_place_marketId']=dat['PLACE']['marketId']
 else:
  out['betfair_place']=None
  out['betfair_place_marketId']=None
 if "MATCH_BET" in dat:
  out['betfair_avb']=dat['MATCH_BET']['marketData']
  out['betfair_avb_marketId']=dat['MATCH_BET']['marketId']
 else:
  out['betfair_avb']=None
  out['betfair_avb_marketId']=None
 return out

def attheraces_list():
 r=requests.get("http://www.attheraces.com/racecards")
 soup=BeautifulSoup(r.text,"html.parser")
 out={}
 for t1 in soup.find_all(class_="panel uk")+soup.find_all(class_="panel eire"):
  for t2 in t1.find_all(class_="meeting"):
   try:
    race_url="http://www.attheraces.com"+t2.get("href")
    race_time=int(time.mktime(dateutil.parser.parse(t2.find("meta",{"itemprop":"startDate"}).get("content").strip()+".000Z").timetuple()))-3600
    out[race_time]=race_url
   except:
    junk=1
 return out

def timeform_dat(atr_url,ladbrokes_url,raceId):
 r=requests.get(atr_url)
 soup=BeautifulSoup(r.text,"html.parser")
 out1={}
 tf_list=["".join([c1 for c1 in c.text.lower().strip() if c1.isalpha()]) for c in soup.find_all(class_="panel-footer bordered")[-1].find_all("a")]
 for t1 in soup.find_all(class_="card-item"):
  horse_number=int(t1.find(class_="card-no-draw__no").text.strip())
  horse_name=t1.find(class_="name summary form-link horse-form-link").text.strip().split("\r")[0].strip()
  if "".join([c for c in horse_name.lower().strip() if c.isalpha()]) in tf_list:
   out1[horse_number]=tf_list.index("".join([c for c in horse_name.lower().strip() if c.isalpha()]))+1
 r=requests.get(ladbrokes_url)
 soup=BeautifulSoup(r.text,"html.parser")
 verdict=soup.find_all(class_="ruled")[0].text.strip().lower()
 out2={}
 for t in soup.find(id="racecard").find_all("tr")[1:]:
  hr_num=int(t.find_all("td")[0].find("strong").text.strip())
  hr_name=t.find_all(class_="name")[0].text.strip()
  hr_name="".join([c for c in hr_name if c.isalpha()])
  out2[hr_name.lower().strip()]=hr_num
 out3={}
 for t in soup.find_all(class_="article-snippet")[-1].find_all("p"):
  try:
   if t.find(class_="highlight3"):
    hr_name=t.find(class_="highlight3").text.strip().lower()
    hr_name="".join([c for c in hr_name if c.isalpha()])   
    out3[out2[hr_name]]=0
    t.strong.decompose()
    if "-" in t.text.strip().split(" ")[0]:
     if "(" not in t.text.strip().split(" ")[0]:
      tmp3="".join([c for c in t.text.strip().split(" ")[0] if c.isdigit() or c=="-"]).replace("-",".").strip()
      param1=round(float(tmp3.split(".")[0])/float(tmp3.split(".")[1]),2)
    if "-" in t.text.strip().split(" ")[1]:
     if "(" not in t.text.strip().split(" ")[1]:
      tmp3="".join([c for c in t.text.strip().split(" ")[1] if c.isdigit() or c=="-"]).replace("-",".").strip()
      param1=round(float(tmp3.split(".")[0])/float(tmp3.split(".")[1]),2)
    out3[out2[hr_name]]=param1
  except:
   junk=1
 r=requests.post("https://www.betfair.com/www/sports/navigation/facet/v1/search?_ak="+str(ak)+"&alt=json",json={"filter":{"marketBettingTypes":["ODDS"],"exchangeIds":[1],"eventTypeIds":[7],"productTypes":["EXCHANGE"],"contentGroup":{"language":"en","regionCode":"ASIA"},"attachments":["MARKET_LITE"],"selectBy":"RANK","raceIds":[str(raceId)]},"textQuery":None,"facets":[],"currencyCode":"USD","locale":"en"})
 win_market_id=[r.json()['attachments']['liteMarkets'][c]['marketId'] for c in r.json()['attachments']['liteMarkets'] if r.json()['attachments']['liteMarkets'][c]['marketType']=="WIN"][0]
 r=requests.get("https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket?_ak="+str(ak)+"&currencyCode=USD&locale=en&marketIds="+str(win_market_id)+"&rollupLimit=4&rollupModel=STAKE&types=MARKET_STATE,MARKET_DESCRIPTION,RUNNER_METADATA,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP")
 fin_out=[]
 for c in r.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners']:
  if int(c['description']['metadata']['CLOTH_NUMBER']) in out1 and int(c['description']['metadata']['CLOTH_NUMBER']) in out3:
   fin_out.append({"param1":out3[int(c['description']['metadata']['CLOTH_NUMBER'])],"timeform_123":out1[int(c['description']['metadata']['CLOTH_NUMBER'])],"odds":round((sorted(c['exchange']['availableToBack'],key=lambda x:x['price'])[-1]['price']+sorted(c['exchange']['availableToLay'],key=lambda x:x['price'])[0]['price'])/2.0,2),"horse_name":c['description']['runnerName'],"horse_number":int(c['description']['metadata']['CLOTH_NUMBER'])}) 
 return sorted(fin_out,key=lambda x:x['param1'],reverse=True)

def ladbrokes365_list():
 r=requests.get("http://ladbrokes.365dm.com/racing/form/racecards/"+str(datetime.datetime.now().strftime("%d-%m-%Y"))+"/")
 soup=BeautifulSoup(r.text,"html.parser")
 r_list={}
 rep=[]
 for t in soup.find_all(class_="rac-cards"):
  try:
   race_time=int(time.mktime(dateutil.parser.parse(datetime.date.today().strftime("%Y-%m-%d")+"T"+t.find(class_="ixt").text.strip()+".000Z").timetuple()))-3600
   race_url="http://ladbrokes.365dm.com"+t.find("a").get("href").strip()
   if race_time not in r_list:
    r_list[race_time]=race_url
  except:
    junk=1
 return r_list

def update_res():
 r=requests.get("http://localhost:9200/uk_hr_all_build5/_doc/_search",json={"from" : 0, "size" : 10000,"query": {"bool": {"must_not": {"exists": {"field": "result_123"}}}}})
 for t in r.json()['hits']['hits']:
  tmp=t['_source']
  if "race_details_dat" in tmp:
   if "betfair_avb" in tmp['race_details_dat'] and tmp['race_details_dat']['betfair_avb'] and "result_avb" not in tmp:
    params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['race_details_dat']['betfair_avb'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
    headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
    r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
    winners=[c['description']['runnerName'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
    if winners:
     tmp['result_avb']=winners[0]
   if "betfair_win" in tmp['race_details_dat'] and tmp['race_details_dat']['betfair_win'] and "result_win" not in tmp:
    params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['race_details_dat']['betfair_win'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
    headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
    r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
    winners=[c['description']['runnerName'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
    if winners:
     tmp['result_win']=winners[0]
   if "betfair_place" in tmp['race_details_dat'] and tmp['race_details_dat']['betfair_place'] and "result_place" not in tmp:
    params = (    ('_ak',ak),    ('currencyCode', 'USD'),    ('locale', 'en'),    ('marketIds',str(tmp['race_details_dat']['betfair_place'])),    ('rollupLimit', '2000'),    ('rollupModel', 'STAKE'),    ('types', 'RUNNER_METADATA,MARKET_STATE,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_DESCRIPTION,RUNNER_SP'),)
    headers = {'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-US,en;q=0.9','User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36 OPR/52.0.2871.40','Accept': 'application/json, text/plain, */*','Referer': 'https://www.betfair.com/exchange/plus/horse-racing/market/1.142540219?nodeId=28677009.1540','Connection': 'keep-alive'}
    r1=requests.get('https://www.betfair.com/www/sports/exchange/readonly/v1/bymarket', headers=headers, params=params)
    winners=[c['description']['runnerName'] for c in r1.json()['eventTypes'][0]['eventNodes'][0]['marketNodes'][0]['runners'] if c['state']['status']=="WINNER"]
    if winners:
     tmp['result_place']=json.dumps(winners)
   junk=requests.post("http://localhost:9200/uk_hr_all_build5/_doc/"+str(t['_id']),json=tmp)

def model_1(inp):
 inp['model_1_prediction']=None
 if 'a_timeform_dat' in inp and inp['a_timeform_dat'] and inp['a_timeform_dat'][0]['odds']>=5.0 and inp['a_timeform_dat'][0]['odds']<=30.0:
  inp['model_1_prediction']=inp['a_timeform_dat'][0]
 return inp

list1=attheraces_list()
list2=betfair_list()
list3=ladbrokes365_list()

common_list=[c for c in list1 if c in list2 and c in list3]
print "Total Races Today",len(common_list)

t=sorted(common_list)[0]

total_dat={}
while True:
  time.sleep(2)
  try:
    update_res()
  except:
    print "-----"
  try:
    for t in sorted(common_list):
      cur_ts=int(time.mktime((datetime.datetime.utcnow().timetuple())))
      if cur_ts>=t-180 and cur_ts<=t+120 and t not in total_dat:
        final_dat={}
        final_dat['timestamp']=int(t)*1000
        final_dat['a_timeform_dat']=timeform_dat(list1[t],list3[t],list2[t])
        bf_dat=betfair_dat(list2[t])
        final_dat['betfair_win_dat']=bf_dat['betfair_win']
        final_dat['betfair_place_dat']=bf_dat['betfair_place']
        final_dat['betfair_avb_dat']=bf_dat['betfair_avb']
        final_dat['race_details_dat']={"ladbrokes365_url":list3[t],"betfair_place":bf_dat['betfair_place_marketId'],"betfair_avb":bf_dat['betfair_avb_marketId'],"attheraces":list1[t],"betfair_win":bf_dat['betfair_win_marketId']}
        final_dat=model_1(final_dat)
        r=requests.post("http://localhost:9200/uk_hr_all_build5/_doc/"+str(final_dat['timestamp']),json=final_dat)
        print list1[t],"Added New Race................."
        total_dat[t]="added"
  except:
    print "error----------------" 



