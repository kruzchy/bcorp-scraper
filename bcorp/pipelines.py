# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd
import boto3
import pandas as pd
from smart_open import open
from scrapy.exceptions import DropItem
from .settings import PROJECT_ROOT
class BcorpPipeline:
    def open_spider(self, spider):
        self.s3 = boto3.resource('s3')
        self.bucket_name = 'bcorp-scraper'
        self.object_key = "scraped.csv"
        path = f"s3://{self.bucket_name}/{self.object_key}"
        self.df = pd.read_csv(open(path))
        print('>>READ DF from S3')
        self.filepath = f'{PROJECT_ROOT}\\scraped.csv'
        self.hasUpdated = False
        # self.df = pd.read_csv(self.filepath)

    def process_item(self, item, spider):
        if self.df['name'].str.contains(item['name']).any():
            print(f'***skipping {item["name"]}')
            # raise DropItem("Duplicate item found: %s" % item)
        else:
            self.hasUpdated = True
            print(f'****appending {item["name"]}')
            # dicc = item.__dict__['_values']
            # fieldnames = dicc.keys()
            # writer = csv.DictWriter(open(self.filepath, 'a', encoding='utf-8'), lineterminator='\n', fieldnames=fieldnames)
            # writer.writerow(dicc)
            # writer.writerow([item[key] for key in item.keys()])
            self.df = self.df.append(item.__dict__['_values'], ignore_index=True, sort=False)
            self.df.to_csv(self.filepath, index=False)

    def close_spider(self, spider):
        if self.hasUpdated:
            self.s3.meta.client.upload_file(self.filepath, self.bucket_name, self.object_key)
            print('>>uploaded Scraped.csv to S3!')
