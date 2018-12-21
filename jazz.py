import requests
import time
import xmltodict, json
import json
from time import strftime
from datetime import datetime
import sys

def out(text):
 o = xmltodict.parse(text)
 d = json.loads(json.dumps(o))
 return d

date=raw_input("Enter Date(yyyymmdd)(press enter default today date)>>>")
if len(date)<7:
 date=datetime.now().strftime('%Y%m%d')
 print "default date today set",date

print ""
num_of_tkt=int(raw_input("Number of Ticket>>>"))
print ""

headers = {'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0.1; ONEPLUS A3003 Build/MMB29M)','Accept':'application/json','Host':'mweb.jazzcinemas.com','Accept-Encoding':'gzip'}
r=requests.post("https://mweb.jazzcinemas.com/DataserviceJson.asmx/GetMovieList_ByType",headers=headers, data = {"lsCinemaId":"100","lsType":"ns"})
t=out(r.text)
movie=[]
itr=1
for i in t['MovieInfoListByType']['MovieInfoItem']['MovieInfo']:
 movie.append([str(itr),i['Movie_strName'],i['Movie_strId']])
 itr=itr+1

for i in movie:
 print i[0],":",i[1]

print ""

smovie=movie[int(raw_input("Enter Movie Number:>>>"))-1]


headers = {'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0.1; ONEPLUS A3003 Build/MMB29M)','Accept':'application/json','Host':'mweb.jazzcinemas.com','Accept-Encoding':'gzip'}
r=requests.post("https://mweb.jazzcinemas.com/DataserviceJson.asmx/GetScheduleDetails_ByMovie_V3", timeout=5,headers=headers, data = {"lsCinemaId":"100","lsMovieId":smovie[2],"lsDate":str(date)+"000000","lnTicketCount":str(num_of_tkt)})
t=out(r.text)
avail=[]
for t1 in t['ScheduleDetails_ByMovie']['Schedule_Details']['Movie_experience']['Screen_Name_ByExp']['ScreenName_ByExperience']:
 if "list" in str(type(t1['SessionTimeByScr']['SessionTime_ByScreen'])):
  for t2 in t1['SessionTimeByScr']['SessionTime_ByScreen']:
   avail.append(t2['Session_Time'])
 else:
  avail.append(t1['SessionTimeByScr']['SessionTime_ByScreen']['Session_Time'])
mov_tim={i+1:sorted(avail)[i] for i in range(len(avail))}
pprint(mov_tim)


print ""
movie_times=raw_input("Enter movie Times>>>")
movie_times=[mov_tim[int(c)] for c in movie_times.split(",")]
print movie_times
print ""



headers = {'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 6.0.1; ONEPLUS A3003 Build/MMB29M)','Accept':'application/json','Host':'mweb.jazzcinemas.com','Accept-Encoding':'gzip'}
r=requests.post("https://mweb.jazzcinemas.com/DataserviceJson.asmx/GetScheduleDetails_ByMovie_V3", timeout=5,headers=headers, data = {"lsCinemaId":"100","lsMovieId":smovie[2],"lsDate":str(date)+"000000","lnTicketCount":str(num_of_tkt)})
t=out(r.text)
avail={}
for t1 in t['ScheduleDetails_ByMovie']['Schedule_Details']['Movie_experience']['Screen_Name_ByExp']['ScreenName_ByExperience']:
 if "list" in str(type(t1['SessionTimeByScr']['SessionTime_ByScreen'])):
  for t2 in t1['SessionTimeByScr']['SessionTime_ByScreen']:
   if int(t2['Available_Seat'])>=num_of_tkt:
    avail[t2['Session_Time']]=int(t2['Available_Seat'])
 else:
  t2=t1['SessionTimeByScr']['SessionTime_ByScreen']
  if int(t2['Available_Seat'])>=num_of_tkt:
   avail[t2['Session_Time']]=int(t2['Available_Seat'])

avail_now=[c for c in movie_times if c in avail]















