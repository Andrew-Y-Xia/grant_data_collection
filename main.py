import collect_data
import create_database
from multiprocessing import Process


'''
YOU MUST SET UP AN SQL SERVER AND ENTER LOG-IN CREDENTIALS FOR THE DATABASE YOU WISH TO COLLECT THE DATA IN TO SUCESSFULLY RUN PROGRAM.


This program requests all data on grants from Dimensions.ai and the nih reporter API
It then collects the data into a prexisting MySQL database with two tables:

TABLE researchers:

researcher_id INT PRIMARY KEY    : An unique ID given to each researcher; IDs start at 0 and ascend
dimensions_id VARCHAR(127)       : The unique ID used for researchers in the Dimensions dataset; This may be blank if researcher has no presence in the Dimensions dataset
nih_ppids JSON                   : A list of NIH IDs used by the NIH; researchers may have more than one NIH ID
first_name VARCHAR(255)          : The researcher's first name
last_name VARCHAR(255)           : The researcher's last name
middle_name VARCHAR(255)         : The researcher's middle name
grant_ids JSON                   : A list of ids of the grants that were received by this researcher
additional_info_dimensions JSON  : Raw data about this researcher that was returned from the dimensions dataset; may be empty if data was retrieved from the nih dataset instead
additional_info_nih JSON         : Raw data about this resesarcher that was returned from the nih dataset; may be empty if data was retrieved from the dimensions dataset instead


TABLE grants:

grant_id INT PRIMARY KEY          : An unique ID given to each grant; IDs start at 0 and ascend
dimensions_grant_id VARCHAR(255)  : The unique ID used for grants in the Dimensions dataset; This may be blank if grant is not recorded in the Dimensions dataset
nih_serial_number VARCHAR(255)    : NIH-designated Serial Number; will be empty if project was not funded by the NIH
project_number VARCHAR(512)       : Project numbers as specified by the dimensions dataset
title VARCHAR (1023)              : Title of grant
is_nih BOOLEAN                    : Indicates whether this grant was funded by the NIH
funding_orgs_name JSON            : Name(s) of funding organization(s)
research_orgs_name JSON           : Names of research organization involved with the grant
funding_usd FLOAT                 : Number of US dollars of funding for the grant
start_date DATETIME               : Start date of the grant
end_date DATETIME                 : End date of the grant
researcher_ids JSON               : List of ids of researchers funded by this grant
additional_info_dimensions JSON   : Raw data about this grant that was returned from the dimensions dataset; may be empty if data was retrieved from the nih dataset instead
additional_info_nih JSON          : Raw data about this grant that was returned from the nih dataset; may be empty if data was retrieved from the dimensions dataset instead

This program will take under a day to run.
'''

def main():
    
    # We can query nih and dimensions servers independently, so we can run them concurrently
    p = Process(target=collect_data.extract_dimensions_data, args=())
    p.start()
    collect_data.extract_nih_data()
    p.join()
    '''
    Above section is equivalent to :
    collect_data.extract_dimenisions_data()
    collect_data.extract_nih_data()
    '''
    
    
    create_database.init_tables()
    create_database.init_dicts()
    
    # Dimensions data must be loaded first, then the nih data
    create_database.load_dimensions_data()
    create_database.load_nih_data()
    
    
if __name__ == "__main__":
    main()
