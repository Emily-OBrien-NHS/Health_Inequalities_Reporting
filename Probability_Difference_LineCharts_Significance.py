# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 11:49:20 2024

@author: PerkinsH
"""


binned = pd.cut(imd_1_2['days_wait'],lst)\
	.reset_index()
	
probs_i12 = ((binned.groupby('days_wait').count())/imd_1_2.loc\
	[imd_1_2['days_wait']<475].shape[0]).reset_index()
	

binned_310 = pd.cut(imd_3_10['days_wait'],lst)\
	.reset_index()
	
probs_i310 = ((binned_310.groupby('days_wait').count())/imd_3_10.loc\
	[imd_3_10['days_wait']<475].shape[0]).reset_index()
	
fig,ax=plt.subplots()
sns.lineplot(probs_i12['index'])
sns.lineplot(probs_i310['index'])

binned_me = pd.cut(me['days_wait'],lst)\
	.reset_index()
	
probs_me = ((binned_me.groupby('days_wait').count())/me.loc\
	[me['days_wait']<475].shape[0]).reset_index()
	

binned_wb = pd.cut(wb['days_wait'],lst)\
	.reset_index()
	
probs_wb = ((binned_wb.groupby('days_wait').count())/wb.loc\
	[wb['days_wait']<475].shape[0]).reset_index()

fig,ax=plt.subplots()
sns.lineplot(probs_me['index'])
sns.lineplot(probs_wb['index'])
	

fig,ax=plt.subplots()
sns.lineplot(probs_i12['index']-probs_i310['index'])
sns.lineplot(probs_me['index']-probs_wb['index'])
plt.hlines(0,0,14,color = 'grey',linestyle = '--')

lst=[]
i=0
while i<20:
	lst.append(25*i)
	i+=1