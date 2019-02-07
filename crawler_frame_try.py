import logging
from datamodel.search.Paichunw_datamodel import PaichunwLink, OnePaichunwUnProcessedLink
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

import sys
from urlparse import urlparse, parse_qs, urljoin
from uuid import uuid4
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(PaichunwLink)
@GetterSetter(OnePaichunwUnProcessedLink)
class CrawlerFrame(IApplication):
    app_id = "Paichunw"

    def __init__(self, frame):
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
        unprocessed_links = self.frame.get_new(OnePaichunwUnProcessedLink)
        if unprocessed_links:
            self.download_links(unprocessed_links)

    def download_links(self, unprocessed_links):
        for link in unprocessed_links:
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(PaichunwLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    

max_url=None
max_outlinks=0

def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    urls=set()
    orig_url=rawDataObj.url
    orig_cont=rawDataObj.content

    if rawDataObj.is_redirected:
        orig_url=rawDataObj.final_url
    for item in BeautifulSoup(orig_cont, "lxml").findAll('a'):
        url=item.get('href')
        url = urljoin(orig_url, url) # Ensure abolute url.
        if is_valid(url): 
            urls.add(url)
        else:
            print ("Invalid url %s" % url).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)

    for url in urls:
        outputLinks.append(url)

    # Record the url that has the most out links
    global max_outlinks
    global max_url
    len_outlinks=len(outputLinks)
    if len_outlinks > max_outlinks:
        max_outlinks=len_outlinks
        max_url=orig_url

    print ("length of output links %d" % len(outputLinks)).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
    print ("max_url: %s" % max_url).encode(sys.stdin.encoding, "replace").decode(sys.stdin.encoding)
    print "max_outlinks: %d" % max_outlinks

    return outputLinks

trap={"http://calendar.ics.uci.edu"}

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    # Check if the url is absolute.
    if not bool(urlparse(url).netloc): return False

    # Check the crwaler trap.
    for t in trap:
        if t in url: return False
        
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
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

