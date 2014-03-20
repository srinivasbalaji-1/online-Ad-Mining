create table queries(qid INTEGER PRIMARY KEY,query VARCHAR2(400));
/
create table advertisers(advertiserid INTEGER PRIMARY KEY,ctc FLOAT,budget FLOAT);
/
create table keywords(advertiserid INTEGER,keyword varchar2(100),bid FLOAT,PRIMARY KEY(advertiserid,keyword),FOREIGN KEY(advertiserid) REFERENCES advertisers);
/
Alter table advertisers add balance float;
/
create or replace type ad AS object(
counter INTEGER,
balance FLOAT,
lbid FLOAT
);
/
create or replace type adrow AS object(
qid INTEGER,
qualscore float,
aid INTEGER,
balance FLOAT,
budget FLOAT,
ctc FLOAT
);
/
create or replace type adtable IS TABLE OF adrow;
/
create or replace type b_row AS OBJECT(
tokens varchar2(32767)
);
/
create or replace type b_tab IS TABLE of b_row;
/

create or replace procedure getADS(result OUT sys_refcursor,algo IN INTEGER,ranker in INTEGER) IS
c sys_refcursor;
qry sys_refcursor;
temp sys_refcursor;
qid INTEGER;
rank INTEGER;
q varchar2(400);
words b_tab;
arr apex_application_global.vc_arr2;
TYPE aidcount IS TABLE OF ad INDEX BY PLS_INTEGER;
adtable1 adtable := adtable();
admain adtable := adtable();
adcount aidcount;
genbal FLOAT;
qd INTEGER;
sim FLOAT;
sim1 INTEGER;
ctc FLOAT;
aid INTEGER;
bid FLOAT;
budget FLOAT;
tracker FLOAT;
BEGIN
 update advertisers set balance=budget;
 open qry for select * from (select * from queries order by qid);
 loop
  FETCH qry INTO qid,q;
  exit when qry%NOTFOUND;
  words := b_tab();
  arr := apex_util.string_to_table(q,' ');
  for i in 1..arr.count
  loop
  words.extend;
  words(words.last) := b_row(arr(i));
 end loop;
 if algo=3 or algo=1 then
 open c for select qid,BDSum.bidsum*(ABsum.AB/(sqrt(Bsum.B)*sqrt(Asum.A)))*ad.ctc AS qual,ad.ctc,ad.advertiserid,BDsum.bidsum bidval,ad.budget from
(select sum(s.sumB2) B from (select count(*)*count(*) sumB2 from table(words) group by tokens) s) Bsum, (select advertiserid,count(*) A from keywords group by advertiserid) Asum, (select advertiserid,count(*) AB from
table(words) t,keywords k
where k.keyword=t.tokens group by advertiserid) ABsum,(select advertiserid,sum(bid) bidsum from (select DISTINCT tokens from table(words)) t1,keywords k2 where t1.tokens=k2.keyword group by advertiserid) BDsum,
advertisers ad,keywords k1 where ad.advertiserid=k1.advertiserid and ABsum.advertiserid=ad.advertiserid and asum.advertiserid=ad.advertiserid and bdsum.advertiserid=ad.advertiserid and ad.balance>=BDsum.bidsum group by
ad.advertiserid,ad.budget,ABsum.AB,Bsum.B,Asum.A,ad.ctc,BDsum.bidsum;
elsif algo=2 then
 open c for select qid,(ABsum.AB/(sqrt(Bsum.B)*sqrt(Asum.A)))*ad.ctc AS qual,ad.ctc,ad.advertiserid,BDsum.bidsum bidval,ad.budget from
(select sum(s.sumB2) B from (select count(*)*count(*) sumB2 from table(words) group by tokens) s) Bsum, (select advertiserid,count(*) A from keywords group by advertiserid) Asum, (select advertiserid,count(*) AB from
table(words) t,keywords k
where k.keyword=t.tokens group by advertiserid) ABsum,(select advertiserid,sum(bid) bidsum from (select DISTINCT tokens from table(words)) t1,keywords k2 where t1.tokens=k2.keyword group by advertiserid) BDsum,
advertisers ad,keywords k1 where ad.advertiserid=k1.advertiserid and ABsum.advertiserid=ad.advertiserid and asum.advertiserid=ad.advertiserid and bdsum.advertiserid=ad.advertiserid and ad.balance>=BDsum.bidsum group by
ad.advertiserid,ad.budget,ABsum.AB,Bsum.B,Asum.A,ad.ctc,BDsum.bidsum;
 elsif algo=4 or algo=6 then
  open c for select qid,bvalue.obid*(ABsum.AB/(sqrt(Bsum.B)*sqrt(Asum.A)))*ad.ctc,ad.ctc,ad.advertiserid,bvalue.spabid bidval,ad.budget from
(select sum(s.sumB2) B from (select count(*)*count(*) sumB2 from table(words) group by tokens) s) Bsum, (select advertiserid,count(*) A from keywords group by advertiserid) Asum, (select advertiserid,count(*) AB from
table(words) t,keywords k where k.keyword=t.tokens group by advertiserid) ABsum,(select ads.advertiserid AS advertiserid1,temp.j2 AS spabid,temp.j1 AS obid from (select j.bidsum AS j1,nvl(lead(j.bidsum) over (order by j.bidsum
desc),j.bidsum) AS j2 from (select DISTINCT sum(bid) bidsum
from (select DISTINCT tokens from table(words)) t1,keywords k2,advertisers a6 where t1.tokens=k2.keyword and a6.advertiserid=k2.advertiserid group by k2.advertiserid,a6.balance having sum(bid)<=a6.balance) j) temp,(select k2.advertiserid AS advertiserid,sum(bid) bidsum1 from (select DISTINCT tokens from table(words))
t1,keywords k2,advertisers a7 where t1.tokens=k2.keyword and a7.advertiserid=k2.advertiserid group by k2.advertiserid,a7.balance having a7.balance-sum(bid)>=0) ads where ads.bidsum1=temp.j1) bvalue ,advertisers ad,keywords k1 where ad.advertiserid=k1.advertiserid and ABsum.advertiserid=ad.advertiserid and asum.advertiserid=ad.advertiserid and
 ad.balance>=bvalue.obid and bvalue.advertiserid1=ad.advertiserid group by ad.advertiserid,ad.budget,ABsum.AB,Bsum.B,Asum.A,ad.ctc,bvalue.spabid,bvalue.obid;
 else
 open c for select qid,(ABsum.AB/(sqrt(Bsum.B)*sqrt(Asum.A)))*ad.ctc,ad.ctc,ad.advertiserid,bvalue.spabid bidval,ad.budget from
(select sum(s.sumB2) B from (select count(*)*count(*) sumB2 from table(words) group by tokens) s) Bsum, (select advertiserid,count(*) A from keywords group by advertiserid) Asum, (select advertiserid,count(*) AB from
table(words) t,keywords k where k.keyword=t.tokens group by advertiserid) ABsum,(select ads.advertiserid AS advertiserid1,temp.j2 AS spabid,temp.j1 AS obid from (select j.bidsum AS j1,nvl(lead(j.bidsum) over (order by j.bidsum
desc),j.bidsum) AS j2 from (select DISTINCT sum(bid) bidsum
from (select DISTINCT tokens from table(words)) t1,keywords k2,advertisers a6 where t1.tokens=k2.keyword and a6.advertiserid=k2.advertiserid group by k2.advertiserid,a6.balance having sum(bid)<=a6.balance) j) temp,(select k2.advertiserid AS advertiserid,sum(bid) bidsum1 from (select DISTINCT tokens from table(words))
t1,keywords k2,advertisers a7 where t1.tokens=k2.keyword and a7.advertiserid=k2.advertiserid group by k2.advertiserid,a7.balance having a7.balance-sum(bid)>=0) ads where ads.bidsum1=temp.j1) bvalue ,advertisers ad,keywords k1 where ad.advertiserid=k1.advertiserid and ABsum.advertiserid=ad.advertiserid and asum.advertiserid=ad.advertiserid and
 ad.balance>=bvalue.obid and bvalue.advertiserid1=ad.advertiserid group by ad.advertiserid,ad.budget,ABsum.AB,Bsum.B,Asum.A,ad.ctc,bvalue.spabid,bvalue.obid;
 end if;
 loop
  fetch c into qd,sim,ctc,aid,bid,budget;
  exit when c%NOTFOUND;
  if adcount.exists(aid) then
   tracker := adcount(aid).counter;
   if mod(adcount(aid).counter,100) < ctc*100 then
    if adcount(aid).balance-bid >=0 then
     adtable1.extend;
     adcount(aid).counter := adcount(aid).counter +1;
     tracker:=adcount(aid).balance;
     adcount(aid).balance := adcount(aid).balance-bid;
     adcount(aid).lbid := bid;
     if mod(algo,3) = 1 then
       adtable1(adtable1.last) := adrow(qd,sim,aid,adcount(aid).balance,budget,ctc);
     elsif mod(algo,3) = 2 then
      adtable1(adtable1.last) := adrow(qd,sim*(adcount(aid).balance+bid),aid,adcount(aid).balance,budget,ctc);
     else
      genbal := (1-exp((-1)*((adcount(aid).balance+bid)/budget)));
      adtable1(adtable1.last) := adrow(qd,sim*genbal,aid,adcount(aid).balance,budget,ctc);
    end if;
    end if;
   elsif adcount(aid).balance-bid >=0 then
    adtable1.extend;
    adcount(aid).counter := adcount(aid).counter +1;
    if mod(algo,3) = 1 then
     adtable1(adtable1.last) := adrow(qd,sim,aid,adcount(aid).balance,budget,ctc);
    elsif mod(algo,3) = 2 then
     adtable1(adtable1.last) := adrow(qd,sim*(adcount(aid).balance),aid,adcount(aid).balance,budget,ctc);
    else
     genbal := (1-exp((-1)*(adcount(aid).balance/budget)));
     adtable1(adtable1.last) := adrow(qd,sim*genbal,aid,adcount(aid).balance,budget,ctc);
    end if;
  end if;
  else
   adcount(aid) := ad(1,budget-bid,bid);
   tracker := adcount(aid).counter;
   adtable1.extend;
    if mod(algo,3) = 1 then
     adtable1(adtable1.last) := adrow(qd,sim,aid,adcount(aid).balance,budget,ctc);
    elsif mod(algo,3) = 2 then
     adtable1(adtable1.last) := adrow(qd,sim*(adcount(aid).balance+bid),aid,adcount(aid).balance,budget,ctc);
    else
     genbal := (1-exp((-1)*((adcount(aid).balance+bid)/budget)));
     adtable1(adtable1.last) := adrow(qd,sim*genbal,aid,adcount(aid).balance,budget,ctc);
    end if;

  end if;

  --dbms_output.put_line(qd||' '||sim||' '||ctc||' '||bid||' '||aid||' '||budget);
  end loop;
  open temp for select qid,dense_rank() over (partition by qid order by qualscore desc,aid asc) AS rank,aid,balance,budget,ctc from table(adtable1);
  loop
   fetch temp into qd,sim1,aid,bid,budget,ctc;
   exit when temp%NOTFOUND;
   if sim1 <= ranker then
    admain.extend;
    admain(admain.last) := adrow(qd,sim1,aid,bid,budget,ctc);
    update advertisers set balance=bid where advertiserID=aid;
   elsif mod(adcount(aid).counter-1,100) < ctc*100 then
    adcount(aid).counter:=adcount(aid).counter-1;
    adcount(aid).balance := adcount(aid).balance + adcount(aid).lbid;
   else
    adcount(aid).counter := adcount(aid).counter-1;
   end if;
  end loop;
  words.delete;
  adtable1.delete;
 end loop;
 open result for select * from table(admain);
 
 --adtable1.delete;
END;
/
exit;
