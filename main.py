# This guy brings it all together


from report_diff import prepare_incumbent_frame, get_units_differences
from data_digest import digest, database_dump
from scraper import scrape_csv, get_file_name

import time

import sys
reload(sys)  
sys.setdefaultencoding('UTF8')

from creds import username, password

renew_name = "renew"
gen_name = "gen"

remote_location = 'mysql+pymysql://'+username+':'+password+'@bluedeltapjm.cskdhnmb36bo.us-east-1.rds.amazonaws.com/pjm'

if __name__ == '__main__':
    
    # Timer started
    start = time.time()
    print "Please ensure security permissions are in order for web server."

    # Scrape from GATS and download locally as CSV
    scrape_csv(renew_name)
    scrape_csv(gen_name)

    # Data munging 
    all_frame = digest(get_file_name(renew_name), get_file_name(gen_name))

    # Analyze and report differences - currently just from local location
    print "Analyzing differences......."
    incumbent_frame = prepare_incumbent_frame(remote_location)
    get_units_differences(incumbent_frame, all_frame)
    print "Results have been emailed."

    # Database dump to both local and remote
    print "Updating remote database ......"
    result = database_dump(all_frame, remote_location)
    
    # report time
    end = time.time()
    print "That's a wrap!!! Total time elapsed: "
    print (end - start), "seconds"


