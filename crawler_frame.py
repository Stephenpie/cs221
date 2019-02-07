import logging
from datamodel.search.Paichunw_datamodel import PaichunwLink, OnePaichunwUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4

# store all the global varaibles
from function import save_data, load_data
import json

# loading global variable
file_name = 'data.txt'
pages_count, MaxOutputLink, invalid_count, subdomain = load_data(file_name)

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(PaichunwLink)
@GetterSetter(OnePaichunwUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "Paichunw"

    def __init__(self, frame):
        #self.starttime = time()
        self.app_id = "Paichunw"
        self.frame = frame


    def initialize(self):
        self.count = 0
        links = self.frame.get_new(OnePaichunwUnProcessedLink)
        if len(links) > 0:
            print "Resuming from the previous state."
            self.download_links(links)
        else:
            l = PaichunwLink("http://www.ics.uci.edu/")
            print l.full_url
            self.frame.add(l)

    def update(self):
        global invalid_count
        global pages_count
        global subdomain
        unprocessed_links = self.frame.get_new(OnePaichunwUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            if not is_valid(link.full_url):
                invalid_count += 1
                print "-----", link.full_url, " not_valid_URL"
            else:
                print "Got a link to download:", link.full_url
                downloaded = link.download()
                links = extract_next_links(downloaded)

                for l in links:
                    if is_valid(l):
                        self.frame.add(PaichunwLink(l))

                # subdomain
                parsed=urlparse(link.full_url)
                if parsed.netloc in subdomain:
                    subdomain[parsed.netloc]+=1
                else:
                    subdomain[parsed.netloc]=1

                print "-----this URL end"

                # count how many pages visited
                pages_count += 1
                print "---", pages_count
                if pages_count > 3000:
                    writeFile()
                    print '-----END!!!!!!!!'

        print('---- all compete')
            writeFile()

    def shutdown(self):
        writeFile()
        save_data(file_name, pages_count, MaxOutputLink, invalid_count, subdomain)
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []
    global MaxOutputLink
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''

    if rawDataObj.is_redirected:
        parsed = urlparse(rawDataObj.final_url)
    else:
        parsed = urlparse(rawDataObj.url)

    url_prefix = parsed.scheme + '://' + parsed.netloc

    if len(rawDataObj.content)==0:
        return outputLinks

    doc = lxml.html.fromstring(rawDataObj.content)  

    links = doc.xpath('//a/@href')

    for link in links:
        o_link = ''
        if link.startswith('/'):
            o_link = url_prefix + link
        elif link.startswith('http') or link.startswith('https'):
            o_link = link
        elif link.startswith('#'):
            pass
        else:
            o_link = url_prefix + '/' + link
        if o_link:
            outputLinks.append(o_link)

    print "there are ", len(outputLinks), "of outputlinks"

    if len(outputLinks) > MaxOutputLink[1]:
        MaxOutputLink = [rawDataObj.url, len(outputLinks)]

    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    d = set()
    if 'wics.ics.uci.edu/events' in url.lower():
        return False

    parsed = urlparse(url)

    if parsed.scheme not in set(["http", "https"]):
        return False

    query = parsed.query
    if 'login' in query.lower() or 'edit' in query.lower():
        return False

    path = parsed.path.split('/')

    if len(path) > 10 or len(query) > 30:
        return False

    for p in path:
        if "calendar" in p: 
            return False
        elif '..' in p or 'mailto' in p or '@' in p or 'brownbags' in p:
            return False
        elif p in d:
            return False
        else:
            d.add(p)

    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False

def writeFile():
    file = open("result_file.txt","w") 
    for k in subdomain.keys():
        file.write("subdomain Name: " + k + "\n")
        file.write("Number of URLs: " + str(subdomain[k]) + "\n")

    file.write("Invalid Links: " + str(invalid_count) + "\n")
    file.write(MaxOutputLink[0]+" has the most out links: " + str(MaxOutputLink[1]))
    file.close()
