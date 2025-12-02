import subprocess
import sys
import re

def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """

    # Receive Phi operands (assumes that all lists are comma-separated)
    # stores phi operands
    phi = {
        "S": [], # list of all projected attributes and aggregates in SELECT
        "n": 0, # number of grouping variables
        "V": [], # list of grouping attributes before 
        "F": [], # aggregates for each group (both grouping attributes' and grouping variables')
        "sigma": [], # predicates for each grouping variable specified in SUCH THAT
        "G": "" # predicate in HAVING 
    } 

    # If no filepath was given in the command line, then we assume the user will input operands manually
    if len(sys.argv) == 1:
        print("No file given")
        # ask user for phi operands
        S = input("List all select attributes: ")
        n = input("Number of grouping variables: ")
        V = input("List all grouping attributes: ")
        F = input("List all aggregates: ")
        sigma = input("List grouping variable predicates: ")
        G = input("List predicates for output of GROUP BY: ")
    elif len(sys.argv) == 2:
        # read phi operands from file that the user provided
        with open(f"{sys.argv[1]}", "r") as file:
            S = file.readline().strip()
            S = S.split(": ")[-1]

            n = file.readline().strip()
            n = n.split(": ")[-1]
            n = int(n)

            V = file.readline().strip()
            V = V.split(": ")[-1]

            F = file.readline().strip()
            F = F.split(": ")[-1]
            
            sigma = file.readline().strip()
            sigma = sigma.split(": ")[-1]

            G = file.readline().strip()
            G = G.split(": ")[-1]
    else:
        print("Error: Too many arguments")
        exit()
    
    # print(f"S: {S}\nn: {n}\nV: {V}\nF: {F}\nsigma: {sigma}\nG: {G}")
    
    phi["S"] = S.split(", ")
    phi["n"] = n
    phi["V"] = V.split(", ")
    phi["F"] = [[]] * (n + 1) # create an empty list to store aggregates for each of the n grouping variables +1 list for the aggregates for the standard SQL groups which would be at index 0
    F = F.split(", ")
    # iterate through each aggregate and add them to the list associated with the matching grouping variable
    for agg in F:
        idx = int(agg[0]) # compute what grouping variable the current aggregate is for (idx = 0 are the standard SQL groups)
        phi["F"][idx] = phi["F"][idx] + [agg]
    phi["sigma"] = sigma.split(", ")
    phi["G"] = G 
    
    # print(f"S: {S}\nn: {n}\nV: {V}\nF: {F}\nsigma: {sigma}\nG: {G}")

    # TODO generate the code to create the H table
    mf_struct = "mf_struct = {}"
    

    # Define helper functions
    # Checks whether attribute values in a given row exist within a row in mf_struct
    lookup = """
def lookup(cur: dict, struct: dict, attrs: list) -> int: 
    \"""
    Search for a given "group by" attribute value(s) in mf_struct. 
    If the value(s) doesn't exist then return -1, else return the index for that row.

    :param cur: Current row in the mf_struct.
    :param struct: The mf_struct.
    :param attrs: List of the grouping attributes that make up the key in the mf_struct.
    :return: Either the index of the matching row in mf_struct or -1 if not found.
    \"""

    key = () # compute key using row's grouping attribute values
    for attr in attrs:
        key += (cur[attr],)
    if key in struct.keys():
        return True
    return False

"""
    # adds a new row in mf_struct
    add = f"""
def add(cur: dict, struct: dict, attrs: list, aggs: list):
    \"""
    Adds new row to mf_struct.

    :param cur: Current row from base table.
    :param struct: The mf_struct the new row goes to.
    :param attrs: List of attributes whose values from cur will get added to the new row
    :param aggs: List of aggregates for each grouping variable
    \"""
    key = () # compute key using row's grouping attribute values
    for attr in attrs:
        key += (cur[attr],)
    value = dict()
    for agg in aggs:
        value[agg] = 0
    struct[key] = value

"""
    
    output = """
def output(struct: dict, attrs: list):
    \"""
    Print the rows of a given mf_struct.
    mf_struct's keys are the grouping attribute values themselves. Thus, we convert the keys into a dictionary where the key is the attribute's name (in attrs) and the value is the attribute's value (i.e. mf_struct key). Since the mf_struct's values are dictionaries that store the aggregates, we can combine the two dictionaries to produce a row of the mf_struct consisting of the grouping attributes and the aggregates.

    Example:\n
    For each mf_struct key-value pair -> 
    ("Sam") : {
         "1_sum_quant" : 10,
         "2_avg_quant" : 20,
    }\n
    Convert into a dictionary ->
    {"cust": "Sam", "1_sum_quant" : 10, "2_avg_quant" : 20}

    :param struct: The mf_struct we want to print.
    :param attrs: List of grouping attributes names.
    \"""
    
    ret = [] # stores rows of mf_struct
    # iterate through each entry of mf_struct
    for keys in struct.keys():
        d = {} # initialize a new dictionary that'll stores the row corresponding to the current entry 
        # a key is a tuple so we iterate through each key and map them with their corresponding attribute name
        for key, attr in zip(keys, attrs):
            d[attr] = key
        d.update(struct.get(keys)) # combine the entry's dictionary with the dictionary associated to that entry's value
        ret.append(d) # add it to the list of rows  
    print(tabulate.tabulate(ret, headers=\"keys\", tablefmt=\"psql\")) # print the final table
    """

    
    update = """
def update(row: dict, struct: dict, attrs: list, aggs: list, cond: str):
    \"""
    Updates the rows in mf_struct that are related to the given row.

    :param row: Current row from the base table.
    :param struct: The mf_struct that we're updating.
    :param attrs: Grouping attributes that define the keys of mf_struct
    :param aggs: Aggregates that are being computed for the grouping variable
    :param cond: Condition that define the grouping variable's range
    \"""
    # print(f"Pred: {preds}, Aggs: {aggs}, Attrs: {attrs}")
    
    # construct key that defines a grouping variable's range
    key = ()
    for attr in attrs:
        key += (row[attr],)
    row["state"]="NY"
    row["quant"] = 100
    print(f"Condition: {cond}")
    if eval(cond):
        print("Success!")
        for agg in aggs:
            (var, op, att) = agg.split("_")
            match op:
                case 'sum':
                    struct[key][agg] += row[att]
                case 'count':
                    struct[key][agg] += 1
                case 'max':
                    struct[key][agg] = max(struct[key], row[att])
                case 'min':
                    struct[key][agg] = min(struct[key], row[att])
                case 'avg':
                    struct[key][agg] = 0
        exit()
    else:
        print("Failure")
        exit()
        

    # iterate through mf_struct to identify rows that satisfy grouping variable's range w.r.t the given row
    # entry = struct.get(key)
    
    """
    # TODO generate the code that implements the evaluation algorithm
    # perform n + 1 scans
    
    print(phi["sigma"])
    conds = [] # stores the conditions from the transformed predicates
    # convert each predicate into a condition for an if-block
    for pred in phi["sigma"]:
        # x.state="NY" -> row["state"] == "NY"
        pred = re.sub(r"\d+\.(\w*)", r"row['\1']", pred) # encompass attribute with row[] where row is the variable for the current row  
        # replace all occurrences of '=' with "==" \1 refers to capture group 1 (i.e. [^<|>])
        pred = re.sub(r"([^<|>])=", r"\1==", pred) 
        # print(f"Pred: {pred}")
        conds.append(pred)    
    
    for cond in conds:
        print(cond)    

    body = f"""
    table = cur.fetchall() # store the SQL query output into a list so that it can be scanned multiple times
    for i in range({n + 1}):
        for row in table:
            # scan 0 adds rows with distinct grouping attributes 
            if i == 0:
                exists = lookup(row, mf_struct, {phi["V"]})
                if not exists:
                    add(row, mf_struct, {phi["V"]}, {F})
                update(row, mf_struct, {phi["V"]}, {phi["F"]}[i], "True") # update the rows in mf_struct corresponding to i=0 (aggregates over the standard SQL groups)
            else:
                update(row, mf_struct, {phi["V"]}, {phi["F"]}[i], {conds}[i-1]) # update the rows in mf_struct corresponding to i!=0 (aggregates over the grouping variables)             

    output(mf_struct, {phi["V"]})
    print(f"Entries: {{len(mf_struct.keys())}}")

    """
    
    # body = """
    # for row in cur:
    #     if row['quant'] > 10:
    #         _global.append(row)
    # """
    


    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

# Helper functions
{lookup}
{add}
{output}
{update}
def query():
    load_dotenv() # reads the .env file

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales") # prints all the rows in the data table
    
    _global = []
    {mf_struct}
    {body}

    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql") # returns data as a table

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """
    
    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # COMMENTED OUT FOR TESTING PURPOSES
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()
