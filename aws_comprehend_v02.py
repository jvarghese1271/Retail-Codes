# -*- coding: utf-8 -*-
"""
Created on Wed May 29 09:33:48 2019

@author: jvarghese1271
"""

import pandas as pd
from collections import OrderedDict
import requests
import boto3 
import argparse, datetime
import time
import numpy as np


def main():
	

	parser = argparse.ArgumentParser()
	parser.add_argument('s3_bucket', help='s3 bucket in which pulled data is')
	parser.add_argument('input_loc', help='s3 location at which pulled data is')
	parser.add_argument('output_loc', help='location in which ouput will be dumped')
	args = parser.parse_args()

	s3 = boto3.resource('s3')

	comprehend = boto3.client('comprehend', region_name='us-east-1')

	obj_lst = list(s3.Bucket(args.s3_bucket).objects.filter(Prefix = args.input_loc))
	
	comp_pull_start_tm = datetime.datetime.now()

	

	for obj in obj_lst:

		file_key = obj.key

		pull_start_tm = datetime.datetime.now()

		data_location = 's3://{}/{}'.format(args.s3_bucket, file_key)
		print(data_location)
		data1 = pd.read_csv(data_location,lineterminator='\n')
		data2=pd.DataFrame(data1)
		#data2=data2.sample(frac=0.05)#only when sampling required
		#gender=data2['gender']	
		#PostType=data2['threadEntryType']
		#authorid=data2['twitterAuthorId']
		posts=[]
		sentiments = []
		positive = []
		negative = []
		neutral = []
		mixed=[]
		gender=[]
		PostType=[]
		authorid=[]


		for i in range(len(data2)):
			d = data2['fullText'].iloc[i]
			g1=data2['gender'].iloc[i]
			p1=data2['threadEntryType'].iloc[i]
			a1=data2['twitterAuthorId'].iloc[i]
			try:
				res = comprehend.detect_sentiment(Text=d, LanguageCode='en')
			except:
				res={'Sentiment':"notfound",'SentimentScore':{'Positive':np.nan,'Negative':np.nan,'Neutral':np.nan,'Mixed':np.nan}}
				print ("error caught")

			s = res.get('Sentiment')
			p = res.get('SentimentScore')['Positive']
			neg = res.get('SentimentScore')['Negative']
			neu = res.get('SentimentScore')['Neutral']
			mix= res.get('SentimentScore')['Mixed']
			posts.append(d)
			gender.append(g1)
			PostType.append(p1)
			authorid.append(a1)
			sentiments.append(s)
			positive.append(p)
			negative.append(neg)
			neutral.append(neu)
			mixed.append(mix)
			if (i+1)%100==0:
				time.sleep(2)
			print(i)


		df=pd.DataFrame()
		#df=pd.DataFrame(posts,columns =['posts'])
		df['posts']=posts
		df['sentiments']=sentiments
		df['positive']=positive
		df['negative']=negative
		df['neutral']=neutral
		df['mixed']=mixed
		df['gender']=gender
		df['author_id']=authorid
		df['PostType']=PostType

		data_key_out = obj.key.split('/')[-1].split('.')[0] + '_sentiment.' + obj.key.split('/')[-1].split('.')[1]
		data_location = args.output_loc + data_key_out

		s3.Bucket(args.s3_bucket).put_object(Key=data_location, Body=df.to_csv(index=False))

		pull_end_tm = datetime.datetime.now()
		print("exported to s3:\\" + data_location + " in " + str((pull_end_tm - pull_start_tm).seconds) + " seconds")


if __name__ == "__main__":
	main()