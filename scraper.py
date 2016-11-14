
import sys
reload(sys)  
sys.setdefaultencoding('UTF8')

import pandas as pd
import argparse
import mechanicalsoup

from datetime import datetime

import os

RENEW_URL = "https://gats.pjm-eis.com/gats2/PublicReports/RenewableGeneratorsRegisteredinGATS"
GEN_URL = "https://gats.pjm-eis.com/gats2/PublicReports/GATSGenerators"


def scrape_csv(frame_choice):

    if frame_choice == "renew":
        url = RENEW_URL

    elif frame_choice == "gen":
        url = GEN_URL

    else:
        ValueError 

    # Set browser / Get HTML
    browser = mechanicalsoup.Browser()
    gats_page = browser.get(url)

    # Make export form and pull CSV
    export_form =gats_page.soup.select("#frmExportTo")[0]
    csv_response=browser.submit(export_form, gats_page.url)

    # Use helper function to name file
    file_name = get_file_name(frame_choice)

    # Save file locally and then pull file back into pandas
    with open(file_name, 'w') as f:
        f.write(csv_response.text)

    return None

def get_file_name(frame_choice):

    # Name the file based on today's date
    day = str(datetime.now().date().day)
    month = str(datetime.now().date().month)
    year = str(datetime.now().date().year)
    file_name = frame_choice+"_"+month+"_"+day+"_"+year+".csv"

    return file_name



