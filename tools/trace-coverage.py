#!/usr/bin/env python3
# 00 §7: every outbound call and every untraced decision function. Usage: trace-coverage.py <project-dir>
import re,sys,os
C=re.compile(r'\.(retrieve|exchangeToMono|exchangeToFlux)\(|[Tt]emplate\w*(\(\))?\.(exchange|postFor\w+|getFor\w+|put|delete|patchFor\w+|send|convertAndSend)\(|[bB]ridge\.send\(')
T=re.compile(r'setAttribute\(|RequestSpan\.\w+\(|SpanOutcome\.\w+\(|recordException|\.tag\(')
F=re.compile(r'catch|onError|doOnError|onStatus|exceptionally|recover|nonFatal|failure|SpanOutcome')
D=re.compile(r'\bif ?\(|\bswitch ?\(|\.orElse|\.filter\(|onErrorResume|onErrorReturn|\bcatch ?\(| \? ')
X=re.compile(r'/(dto|model|models|entity|entities|constant|constants|generated)/')
R=re.compile(r'^(\s+)(public|private|protected) ')
S={}
for d,_,fs in os.walk(sys.argv[1]):
  if '/src/main/java' in d:
    for f in fs:
      if f.endswith('.java'): S[os.path.join(d,f)]=open(os.path.join(d,f),errors='ignore').read().split('\n')
def ms(L):
  ind=next((R.match(l).group(1) for l in L if R.match(l)),'  '); r=[]
  for i,l in enumerate(L):
    if l.startswith(ind) and len(l)>len(ind) and l[len(ind)] not in ' @/*}' and '(' in l and ('=' not in l or l.index('(')<l.index('=')): r.append(i)
  return r
def rng(M,i,n): return max([m for m in M if m<=i] or [0]), min([m for m in M if m>i] or [n])
def name(L,s):
  n=re.search(r'(\w+)\s*\(',L[s]); return n.group(1) if n else '?'
def cov(L,i,M,o):
  s,e=rng(M,i,len(L))
  for j in range(s,e):
    if T.search(L[j]):
      k=re.findall(r'"([^"]*)"',L[j]); k=k[0] if k else L[j].strip()[:30]
      o['f' if F.search(' '.join(L[max(s,j-4):j+1])) else ('b' if j<i else 'a')].append(k)
  return name(L,s)
nc=nm=nd=0
for p,L in sorted(S.items()):
  M=ms(L); seen=set(); cl=os.path.basename(p)[:-5]
  for i,l in enumerate(L):
    if not C.search(l) or l.strip().startswith(('/','*')): continue
    o={'b':[],'a':[],'f':[]}; mn=cov(L,i,M,o)
    if mn in seen: continue
    seen.add(mn); n=0
    for q,Q in S.items():
      if q==p: continue
      MQ=None
      for h,x in enumerate(Q):
        if n<4 and re.search(r'\.'+mn+r'\(',x): MQ=MQ or ms(Q); cov(Q,h,MQ,o); n+=1
    miss=[t for t,ok in (('request',any(k.startswith('call.') for k in o['b'])),('response',o['a']),('failure',o['f'])) if not ok]
    nc+=1; nm+=bool(miss); ks=' '.join(t+':'+','.join(dict.fromkeys(v)) for t,v in o.items() if v)
    print('CALL', cl+'.'+mn+'@'+str(i+1), ('MISSING '+','.join(miss)) if miss else 'OK', ks)
  if X.search(p): continue
  for m in M:
    s,e=rng(M,m,len(L)); b=L[s:e]; k=sum(1 for x in b if D.search(x))
    if k and not any(T.search(x) for x in b): nd+=1; print('DECISION', cl+'.'+name(L,s)+'@'+str(s+1), k, 'branches, no key')
K=re.compile(r'(?:setAttribute|RequestSpan\.(?:set|setIfAbsent|here)|AttributeKey\.\w+Key|SpanOutcome\.nonFatal)\(\s*"([^"]+)"')
V=re.compile(r'^[a-z][a-z0-9]*([._][a-z0-9]+)*$')
nb=0
for p,L in sorted(S.items()):
  for i,l in enumerate(L):
    for k in K.findall(l):
      if not V.match(k) and not k.startswith(('http.request.header.','http.response.header.')): nb+=1; print('BADKEY', os.path.basename(p)[:-5]+'@'+str(i+1), k)
print('SUMMARY calls', nc, 'missing', nm, 'decision-functions-without-key', nd, 'badkeys', nb)
