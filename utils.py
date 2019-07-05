import os
import re
import csv
import glob
import datetime as dt
from pprint import pprint

import requests
from lxml import etree
from PyPDF2 import PdfFileReader, PdfFileWriter
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextLineHorizontal


def mkdir(path):
    """Create new directory
    """
    if not os.path.exists(path):
        os.makedirs(path)


def get_text_objects(layout, t=None):
    """Get PDFMiner LTTextLineHorizontal objects recursively
    """
    if t is None:
        t = []
    try:
        for obj in layout._objs:
            if isinstance(obj, LTTextLineHorizontal):
                t.append(obj)
            else:
                t += get_text_objects(obj)
    except AttributeError:
        pass
    return t


def group_text_objects(tobjs):
    """Group text objects based on x-axis projections
    """
    groups = {}
    tobjs = sorted(tobjs, key=lambda x: (x.bbox[1], x.bbox[0]))
    for t in tobjs:
        if not t.get_text().strip():
            continue
        if not len(groups):
            # initialize a group with the first text object
            groups[1] = {'coords': (t.bbox[0], t.bbox[2]), 'objs': []}
            groups[1]['objs'].append(t.get_text().strip())
        else:
            # find overlaps of text object with existing groups
            overlap = []
            for g, v in groups.items():
                if v['coords'][0] < t.bbox[2] and v['coords'][1] > t.bbox[0]:
                    overlap.append(g)
            if len(overlap) > 1:
                # ignore text object if falls in more than one group
                continue
            elif len(overlap) == 1:
                # add text object to group and update group coordinates
                minx1 = min(groups[overlap[0]]['coords'][0], t.bbox[0])
                minx2 = max(groups[overlap[0]]['coords'][1], t.bbox[2])
                groups[overlap[0]]['coords'] = (minx1, minx2)
                groups[overlap[0]]['objs'].append(t.get_text().strip())
            else:
                # create new group when no overlap is found
                gn = len(groups) + 1
                groups[gn] = {'coords': (t.bbox[0], t.bbox[2]), 'objs': []}
                groups[gn]['objs'].append(t.get_text().strip())
    # create new dict after sorting the groups on their x-coordinates
    ret = {}
    r = 1
    for v in sorted(groups.values(), key=lambda x: x['coords'][0]):
        v['objs'].reverse()
        ret[r] = v
        r += 1
    return ret


def scrape_web(**kwargs):
    """Scrape new PDFs from the IDSP website
    """
    print 'started web scraping task'
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')
    execution_dir = os.path.join(base_dir, execution_date)
    # create run directory
    mkdir(execution_dir)

    weeks = {}
    r = requests.get('http://idsp.nic.in/index4.php?lang=1&level=0&linkid=406&lid=3689')
    tree = etree.fromstring(r.content, etree.HTMLParser())
    table = tree.xpath('//*[@id="cmscontent"]')
    rows = table[0].cssselect('tr')
    links = rows[1].cssselect('a')
    for l in links:
        m = re.search(r'\d+', l.xpath('text()')[0])
        week = m.group(0)
        link = l.xpath('@href')[0]
        weeks[week] = link

    if os.path.exists(os.path.join(base_dir, 'week.csv')):
        # get week number of last scraped PDF
        with open(os.path.join(base_dir, 'week.csv'), 'r') as f:
            reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_ALL, quotechar='"')
            last_week, __ = list(reader)[-1]
    else:
        # fallback if week.csv doesn't exist
        last_week = '7'

    weeks_to_download = filter(lambda x: int(x) > int(last_week), weeks.keys())
    weeks_to_download = sorted(weeks_to_download, key=lambda x: int(x))
    print '{0} new pdfs found on website'.format(len(weeks_to_download))
    if len(weeks_to_download):
        with open(os.path.join(base_dir, 'week.csv'), 'a') as f:
            writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_ALL, quotechar='"')
            for w in weeks_to_download:
                print 'downloading week {0}'.format(w)
                link = weeks[w]
                r = requests.get(link, stream=True)
                pdf_name = os.path.join(execution_dir, link.split('/')[-1])
                with open(pdf_name, 'wb') as pdf:
                    for chunk in r.iter_content(1024):
                        pdf.write(chunk)
                writer.writerow([w, link])


def scrape_pdf(**kwargs):
    """Scrape the downloaded PDFs
    """
    print 'started pdf scraping task'
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')
    execution_dir = os.path.join(base_dir, execution_date)

    # split all PDF pages into single page PDFs
    pdfs = glob.glob(os.path.join(execution_dir, '*.pdf'))
    if len(pdfs) > 0:
        for pdf in pdfs:
            pdf_dir = os.path.join(execution_dir, os.path.splitext(os.path.basename(pdf))[0])
            mkdir(pdf_dir)
            with open(pdf, 'rb') as f:
                infile = PdfFileReader(f, strict=False)
                for i in range(infile.getNumPages()):
                    if i + 1 > 2:
                        pdf_name = os.path.join(pdf_dir, 'page-{0}.pdf'.format(i + 1))
                        page = infile.getPage(i)
                        outfile = PdfFileWriter()
                        outfile.addPage(page)
                        with open(pdf_name, 'wb') as o:
                            outfile.write(o)

        # parse all single page PDFs
        pdfs = glob.glob(os.path.join(execution_dir, '*', 'page-*.pdf'))
        for pdf in pdfs:
            with open(pdf, 'r') as f:
                parser = PDFParser(f)
                document = PDFDocument(parser)
                if not document.is_extractable:
                    raise PDFTextExtractionNotAllowed
                laparams = LAParams(char_margin=1.0, line_margin=0.5, word_margin=0.1, all_texts=True)
                rsrcmgr = PDFResourceManager()
                device = PDFPageAggregator(rsrcmgr, laparams=laparams)
                interpreter = PDFPageInterpreter(rsrcmgr, device)
                for page in PDFPage.create_pages(document):
                    interpreter.process_page(page)
                    layout = device.get_result()
                    text_objects = get_text_objects(layout)
                    groups = group_text_objects(text_objects)
                    # pop first and last column
                    groups.pop(min(groups))
                    groups.pop(max(groups))
                    max_length = max(len(v['objs']) for v in groups.values())
                    data = []
                    # pad all lists with empty strings
                    for g in groups:
                        groups[g]['objs'] = groups[g]['objs'] + [''] * (max_length - len(groups[g]['objs']))
                        data.append(groups[g]['objs'])
                    data = map(list, zip(*data))
                    with open(os.path.splitext(pdf)[0] + '.csv', 'w') as o:
                        writer = csv.writer(o, delimiter=',', quoting=csv.QUOTE_ALL, quotechar='"')
                        for d in data:
                            dd = map(lambda x: x.encode('utf-8'), d)
                            writer.writerow(d)
    else:
        print 'no pdfs to scrape'


def add_to_dataset(**kwargs):
    """Add scraped data to master dataset
    """
    base_dir = kwargs['base_dir']
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')
    execution_dir = os.path.join(base_dir, execution_date)

    # this can be done more nicely using pandas
    # and sorting the csvs based on week and page numbers
    dataset_path = os.path.join(base_dir, 'dataset.csv')
    with open(dataset_path, 'a') as f:
        for csv in glob.glob(os.path.join(execution_dir, '*', '*.csv')):
            with open(csv, 'r') as c:
                for line in c.readlines():
                    f.write(line)
