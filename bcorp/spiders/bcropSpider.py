import scrapy
import json
from requests_html import HTMLSession
from ..items import BcorpItem
import os
class bcorpSpider(scrapy.Spider):
    name = 'bcorp'
    start_urls = ['https://bcorporation.net/directory']
    base_url = 'https://bcorporation.net'

    tmp_obj = dict()
    if os.path.exists('dat.json'):
        with open('dat.json', 'r') as fp:
            tmp_obj = json.load(fp)
    s = HTMLSession()
    r = s.get(start_urls[0])
    new_final_page = r.html.find('.pager-last a', first=True).attrs['href'].split('page=')[1]
    new_final_page = int(new_final_page) if new_final_page else new_final_page
    old_final_page = tmp_obj['final_page']
    new_pages = new_final_page-old_final_page

    dat_obj = {'final_page': new_final_page}
    with open('dat.json', 'w') as fp:
        json.dump(dat_obj, fp)

    print(new_final_page, old_final_page, new_pages)
    def parse(self, response):
        all_containers = response.css('.card__inner')
        # REMEMBER FIRST PAGE IS page 0 so number 197 means page 198

        for container in all_containers:
            url = self.base_url + container.css('a::attr(href)').get()
            yield scrapy.Request(url, callback=self.parse_each_company)

        next_page_num = int(response.css('.next>a::attr(href)').get().split('page=')[1])

        if response.css('.next') and next_page_num < self.new_pages+1:
            next_url = self.base_url + response.css('.next>a::attr(href)').get()
            yield scrapy.Request(next_url, callback=self.parse)

    def parse_each_company(self, response):
        items = BcorpItem()
        top_left = response.css('.profile__topleft--info')
        bottom = response.css('.profile__topleft--bottom')
        items['name'] = top_left.css('h1::text').get()
        items['tagline'] = top_left.css('.field>.field-items>.even::text').get()
        items['certified_since'] = bottom.css('.date-display-single::text').get()
        items['location'] = bottom.css('.field-name-field-country>.field-items>.even::text').get()[10:]
        items['sector'] = bottom.css('.field-name-field-sector>.field-items>.even::text').get()[8:]
        items['b_impact_score'] = response.css('.profile__inner>.circle .text-grey-text::text').get().strip()
        items['url'] = response.url
        items['website'] = response.css('.even a::attr(href)').get()
        tmp = response.css('.field-type-text-with-summary .even>p::text')
        tmp2 = ''
        for i in tmp:
            tmp2 = tmp2+'\n'+i.get()
        items['description'] = tmp2[1:]
        all_score_cards = response.css('.btn-impactreport')
        for index, card in enumerate(all_score_cards):
            li = []
            heading = card.css(".heading4::text").get()
            heading_score = card.css('.circle>div::text').get()
            li.append(f"{heading} {heading_score};")
            for sub_line in card.css('.btn-impactreport--child>.card-block'):
                title = sub_line.css('.pull-left::text').get()
                score = sub_line.css('.pull-right>div::text').get()
                li.append(f"{title} {score};")
            out = ''.join(li)
            items[f"rating{index+1}"] = out
        yield items