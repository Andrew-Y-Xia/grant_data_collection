import mysql.connector
import pickle
import json
from collections import defaultdict
from itertools import islice
from datetime import datetime




GRANT_IDS_INDEX = 6
RESEARCHER_IDS_INDEX = 11

# Connect to the database
# Replace login credentials your credentials
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="PASSWORD",
    database="GrantDataset"
)
mycursor = mydb.cursor()

dimensions_id_to_nih_id, nih_id_to_dimensions_id = None, None

# Returns iterator that goes: 0, 1, 2, 3...
def key_gen():
    id = 0;
    while True:
        yield id
        id += 1

# Custom dictionary for debugging purposes
class loggingdict(dict):
    counter = defaultdict(lambda: 0)
    def __getitem__(self, key):
        if key in self:
            return super().__getitem__(key)
        # print("Key missed: " + str(key))
        loggingdict.counter[key] += 1
        return None


def init_dicts():
    global dimensions_id_to_nih_id; global nih_id_to_dimensions_id;
    dimensions_id_to_nih_id, nih_id_to_dimensions_id = pickle.load(open("dim_nih_ids.p", "rb"))
    dimensions_id_to_nih_id, nih_id_to_dimensions_id = defaultdict(lambda: None, dimensions_id_to_nih_id), defaultdict(lambda: None, nih_id_to_dimensions_id)



def init_tables():
    
    mycursor.execute("""
    CREATE TABLE researchers (
        researcher_id INT PRIMARY KEY, dimensions_id VARCHAR(127), nih_ppids JSON,
        first_name VARCHAR(255), last_name VARCHAR(255), middle_name VARCHAR(255),
        grant_ids JSON,
        additional_info_dimensions JSON,
        additional_info_nih JSON
    );
    """)
    
    mycursor.execute("""
    CREATE TABLE grants (
        grant_id INT PRIMARY KEY, dimensions_grant_id VARCHAR(255), nih_serial_number VARCHAR(255), project_number VARCHAR(512),
        title VARCHAR (1023),
        is_nih BOOLEAN,
        funding_orgs_name JSON, research_orgs_name JSON,
        funding_usd FLOAT,
        start_date DATETIME, end_date DATETIME,
        researcher_ids JSON,
        additional_info_dimensions JSON,
        additional_info_nih JSON
    );
    """)
    
    mycursor.execute("""
    CREATE INDEX idx_dimensions_id
    ON researchers (dimensions_id);
    """)
    mycursor.execute("""
    CREATE INDEX idx_dimensions_grant_id
    ON grants (dimensions_grant_id);
    """)
    mycursor.execute("""
    CREATE INDEX idx_nih_serial_number
    ON grants (nih_serial_number);
    """)
    mycursor.execute("""
    CREATE INDEX idx_project_number
    ON grants (project_number);
    """)

    mycursor.execute("""
    CREATE INDEX idx_names
    ON researchers (first_name, last_name);
    """)


def dim_grants():
    for i in key_gen():
        try:
            for grant in pickle.load(open("full_dimensions" + str(i) + ".p", "rb")):
                yield grant
        except FileNotFoundError:
            return

        
# DOES NOT EXTRACT INFO ON RESEARCHERS
# DOES NOT DETERMINE RESEARCHER_IDS
def extract_dim_grant_info(grant):
    grant = loggingdict(grant)
    
    dimensions_grant_id = grant['id']
    
    project_number = None
    if 'project_numbers' in grant:
        for num in grant['project_numbers']:
            if num['label'] == 'Grant number':
                project_number = num['project_num']
                if len(project_number) > 500:
                    project_number = None
                break
                
    nih_serial_number = None
    # https://www.nimh.nih.gov/funding/grant-writing-and-application-process/grant-mechanisms-and-funding-opportunities
    if project_number != None and project_number[0].lower() in {'r', 'k', 'f', 't', 'p'} and len(project_number) >= 11:
        nih_serial_number = project_number[3:11]
    
    title = grant['title']
    is_nih = False
    funding_orgs_name = grant['funding_org_name']
    research_orgs_name = grant['research_org_names']
    funding_usd = grant['funding_usd']
    start_date = grant['start_date']
    end_date = grant['end_date']
    additional_info_dimensions = grant
    return dimensions_grant_id, project_number, nih_serial_number, title, is_nih, funding_orgs_name, research_orgs_name, \
           funding_usd, start_date, end_date, additional_info_dimensions

# DOES NOT DETERMINE grant_ids
def extract_dim_researcher_info(researcher):
    researcher = loggingdict(researcher)
    dimensions_id = researcher['id']
    nih_ppids = dimensions_id_to_nih_id[dimensions_id]
    first_name = researcher['first_name']
    last_name = researcher['last_name']
    middle_name = researcher['middle_name']
    # Notice no grant_ids
    additional_info_dimensions = researcher
    
    return dimensions_id, nih_ppids, first_name, last_name, middle_name, additional_info_dimensions


def update_dim_researcher_info(mycursor, mydb, researcher, grant_id, researcher_id_gen):
    # Extract info
    dimensions_id, nih_ppids, first_name, last_name, middle_name, additional_info_dimensions = extract_dim_researcher_info(researcher)
    
    results = []
    if dimensions_id != None:
        # Have we already made a record for this researcher?
        mycursor.execute("SELECT * FROM researchers where dimensions_id=\"" + dimensions_id + "\";")
        results = mycursor.fetchall();
        assert len(results) <= 1,  "Researchers database already has duplicates"

    
    # Okay, entry already exists
    # Update researcher entry by adding this grant
    if len(results) == 1:
        grant_ids = json.loads(results[0][GRANT_IDS_INDEX])
        # Add grant
        grant_ids.append(grant_id)
        # Update database
        mycursor.execute("UPDATE researchers SET grant_ids=%s WHERE dimensions_id=%s", (json.dumps(grant_ids), dimensions_id))
        mydb.commit()
        return results[0][0] # returns the researcher_id
    
    # Researcher doesn't exists, so add the entry
    else:
        generated_researcher_id = next(researcher_id_gen)
        mycursor.execute("""INSERT INTO researchers 
            (researcher_id, dimensions_id, nih_ppids,
            first_name, last_name, middle_name, grant_ids,
            additional_info_dimensions) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
                (generated_researcher_id, dimensions_id, json.dumps(nih_ppids),
                 first_name, last_name, middle_name, json.dumps([grant_id]),
                 json.dumps(additional_info_dimensions))
            )
        mydb.commit()
        return generated_researcher_id


grant_id_gen = key_gen()
researcher_id_gen = key_gen()
    

def load_dimensions_data():
    
    # Put dimensions data into database
    for grant in dim_grants():
        dimensions_grant_id, project_number, nih_serial_number, title, is_nih, funding_orgs_name, research_orgs_name, \
               funding_usd, start_date, end_date, additional_info_dimensions = \
        extract_dim_grant_info(grant);

        if dimensions_grant_id == None:
            print(grant)
            continue

        # Check if grant is duplicate
        # Due to the way the dimensions data was collected, there are duplicate grants that must be first eliminated
        mycursor.execute("SELECT * FROM grants where dimensions_grant_id=\"" + dimensions_grant_id + "\";")
        num_entries = len(mycursor.fetchall());
        if num_entries > 0:
            assert num_entries == 1, "Grants database already has duplicates"
            continue


        # Now, we can generate the unique grant_id
        grant_id = next(grant_id_gen)

        # We still need to assign the researcher ids
        researcher_ids = []
        # and extract researcher data
        for researcher in grant['investigators']:
            researcher_id = update_dim_researcher_info(mycursor, mydb, researcher, grant_id, researcher_id_gen)
            researcher_ids.append(researcher_id)

        # Finally, insert grant into database
        mycursor.execute("""INSERT INTO grants 
                (grant_id, dimensions_grant_id, project_number, nih_serial_number,
                 title, is_nih, funding_orgs_name, research_orgs_name,
                 funding_usd, start_date, end_date,
                 researcher_ids, additional_info_dimensions) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                    (grant_id, dimensions_grant_id, project_number, nih_serial_number,
                     title, is_nih, json.dumps(funding_orgs_name), json.dumps(research_orgs_name),
                     funding_usd, start_date, end_date, json.dumps(researcher_ids), json.dumps(additional_info_dimensions))
                )
        mydb.commit()

    print(loggingdict.counter)


"""
LOAD NIH GRANTS
"""

def nih_grants():
    for i in key_gen():
        try:
            for grant in pickle.load(open("nih_recrawl" + str(i) + ".p", "rb")):
                yield grant
        except FileNotFoundError:
            return

def extract_nih_grant_info(grant):
    grant = loggingdict(grant)
    
    nih_serial_number = grant['project_serial_num']
    if nih_serial_number == None and grant['core_project_num'] != None:
        nih_serial_number = grant['core_project_num'][3:]
    project_num = grant['project_num']
    title = grant['project_title']
    is_nih = True
    funding_orgs_name = None
    if grant['agency_ic_fundings'] != None:
        funding_orgs_name = [x['name'] for x in grant['agency_ic_fundings']]
    research_orgs_name = [grant['organization']['org_name']]
    funding_usd = grant['award_amount']
    start_date = grant['project_start_date']
    end_date = grant['project_end_date']
    additional_info_nih = grant
    
    return nih_serial_number, project_num, title, is_nih, funding_orgs_name, research_orgs_name, \
           funding_usd, start_date, end_date, additional_info_nih


# DOES NOT DETERMINE grant_ids
def extract_nih_researcher_info(researcher):
    researcher = loggingdict(researcher)
    
    nih_id = str(researcher['profile_id'])
    first_name = researcher['first_name']
    last_name = researcher['last_name']
    middle_name = researcher['middle_name']
    # Notice no grant_ids
    additional_info_nih = researcher
    
    return nih_id, first_name, last_name, middle_name, additional_info_nih

path= [0] * 10

def update_nih_researcher_info(mycursor, mydb, researcher, grant_id, researcher_id_gen):
    
    nih_id, first_name, last_name, middle_name, additional_info_nih = extract_nih_researcher_info(researcher)
    dimensions_id = nih_id_to_dimensions_id[nih_id]
    
    
    if nih_id == "None" or (first_name == "" and last_name == ""):
        return None
    
    results = []
    if dimensions_id == None:
        # If there's no dimensions equivalent for this nih_ppid, then look up the researcher by name to see if
        # this researcher has already been entered but not from the dimensions side
        mycursor.execute("SELECT researcher_id, grant_ids, nih_ppids FROM researchers WHERE first_name=%s AND last_name=%s", (first_name, last_name))
        possible_matches = mycursor.fetchall();
        for i in possible_matches:
            if nih_id in i[2]:
                results.append(i)
        assert len(results) <= 1,  "Researchers database already has duplicates"
        path[0]+=1
        if (len(results) == 1):
            path[1] += 1
    else:
        # Otherwise, search by the dimensions_id
        mycursor.execute("SELECT researcher_id, grant_ids FROM researchers where dimensions_id=\"" + dimensions_id + "\";")
        results = mycursor.fetchall();
        path[2] += 1
        assert len(results) <= 1,  "Researchers database already has duplicates"

    
    # Okay, entry already exists
    # Update researcher entry by adding this grant
    if len(results) == 1:
        grant_ids = json.loads(results[0][1])
        # Add grant
        grant_ids.append(grant_id)
        # Update database
        mycursor.execute("UPDATE researchers SET grant_ids=%s, additional_info_nih=%s WHERE researcher_id=%s", (json.dumps(grant_ids), json.dumps(additional_info_nih),results[0][0]))
        mydb.commit()
        path[3]+=1
        return results[0][0] # returns the researcher_id of the researcher added
    
    # Researcher doesn't exists, so add the entry
    else:
        generated_researcher_id = next(researcher_id_gen)
        mycursor.execute("""INSERT INTO researchers 
            (researcher_id, dimensions_id, nih_ppids,
            first_name, last_name, middle_name, grant_ids,
            additional_info_nih) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
                (generated_researcher_id, dimensions_id, json.dumps([nih_id]),
                 first_name, last_name, middle_name, json.dumps([grant_id]),
                 json.dumps(additional_info_nih))
            )
        mydb.commit()
        
        path[4] += 1
        return generated_researcher_id # returns the researcher_id of the newly generated researcher
    
    
def load_nih_data():
    
    for grant in nih_grants():
        nih_serial_number, project_num, title, is_nih, funding_orgs_name, research_orgs_name, \
               funding_usd, start_date, end_date, additional_info_nih = extract_nih_grant_info(grant)
        start_date = datetime.strptime(start_date[:10], '%Y-%m-%d')
        if end_date != None:
            end_date = datetime.strptime(end_date[:10], '%Y-%m-%d')

        if nih_serial_number == None:
            continue

        # If data is already included in the dimensions data, then update and continue
        mycursor.execute("SELECT * FROM grants WHERE nih_serial_number=\"" + nih_serial_number + "\" AND is_nih=FALSE;")
        results = mycursor.fetchall();
        if len(results) > 0:
            # Branch should be reached only if this grant exists, and it was added during the dim_grants section
            # assert len(results) == 1, "Grants database already has duplicates"
            if len(results) != 1:
                path[7] += 1
            # Update info
            mycursor.execute("UPDATE grants SET additional_info_nih=%s WHERE nih_serial_number=%s AND is_nih=FALSE;", (json.dumps(additional_info_nih), nih_serial_number))
            mydb.commit()
            path[5]+=1
            continue


        # For the NIH dataset, grant continuations count as multiple entries
        # Find if grant is a continuation
        mycursor.execute("SELECT end_date, funding_usd FROM grants where nih_serial_number=\"" + nih_serial_number + "\" AND is_nih=True")
        results = mycursor.fetchall();
        if len(results) > 0:
            # This branch should only be reached if this grant already exists, but was not added during dimensions phase
            # There should be no duplicates within the nih dataset, so this must be a continuation
            assert len(results) == 1, "Grants database already has duplicates"

            # In the case that the grant entry is a continuation, we need to update the funding_usd and end_date
            old_end_date, old_funding_usd = results[0]
            if end_date != None and old_end_date != None:
                if end_date < old_end_date:
                    end_date = old_end_date
            if funding_usd == None:
                funding_usd = old_funding_usd
            if funding_usd != None and old_funding_usd != None:
                funding_usd += old_funding_usd

            mycursor.execute("""UPDATE grants SET end_date=%s, funding_usd=%s WHERE nih_serial_number=%s;""", (end_date, funding_usd, nih_serial_number))
            mydb.commit()

            path[6] += 1

            continue


        # We've made sure that we're adding a new grant, so we can generate the id now
        grant_id = next(grant_id_gen)


        # Now, update researchers

        researcher_ids = []
        for researcher in grant['principal_investigators']:
            researcher_id = update_nih_researcher_info(mycursor, mydb, researcher, grant_id, researcher_id_gen)
            researcher_ids.append(researcher_id)


        # Finally, add the grant
        mycursor.execute("""INSERT INTO grants 
                (grant_id, nih_serial_number, project_number,
                 title, is_nih, funding_orgs_name, research_orgs_name,
                 funding_usd, start_date, end_date,
                 researcher_ids, additional_info_nih) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                    (grant_id, nih_serial_number, project_num,
                     title, is_nih, json.dumps(funding_orgs_name), json.dumps(research_orgs_name),
                     funding_usd, start_date, end_date, json.dumps(researcher_ids), json.dumps(additional_info_nih))
                )
        mydb.commit()
    
    mycursor.execute("""UPDATE grants SET is_nih=TRUE WHERE additional_info_nih IS NOT NULL AND is_nih=False;""")
    mydb.commit()
    
