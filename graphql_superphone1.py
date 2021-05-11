# An example to get the remaining rate limit using the Github GraphQL API.

import requests
import pandas as pd
from gspread_pandas import upload_with_pd
from dotenv import dotenv_values


config = dotenv_values(".env")

PUBLIC_KEY = config['PUBLIC_KEY']
headers = {"Authorization": "Bearer {}".format(PUBLIC_KEY)}
URL = "https://api.superphone.io/graphql"


# pandas data
# df = pd.DataFrame()
df_contact = pd.DataFrame(columns=[
  'Name', 
  'Gender', 
  'photo link', 
  'City', 
  'State', 
  'Tags', 
  # 'LAST CONTACTED', 
  '$ SPENT', 
  'Email', 
  # 'Messaging', 
  'Mobile', 
  # 'Assigned', 
  'Address',
  'Instagram', 
  'Twitter', 
  'Birthday', 
  # 'Industry', 
  'Notes', 
  # 'Number of Messages Incoming', 
  # 'Number of Messages Outgoing', 
  'Etc',
])

df_conversation = pd.DataFrame(columns=[
  'Contact Name',
  'Messages Incoming',
  'Messages outgoing',
  'Message',
  'Date',
  # 'Link to conversation in Database',
  'Contact Phone Number',
  # 'Phone Number Assigned to User',
])

df_pointers = pd.DataFrame(columns=[
  'Latitude', 
  'Longitude', 
  'Numbers',
  'CX',
  'CY',
])

# cursors list
cursors = []


def run_query(query): # A simple function to use requests.post to make the API call. Note the json= section.
  request = requests.post(URL, json={'query': query}, headers=headers, timeout=30)
  if request.status_code == 200:
    return request.json()
  else:
    raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))


before_pattern = lambda cursor: "" if cursor is None else ", before: \"{}\"".format(cursor)
        
# The GraphQL query (with a few aditional bits included) itself defined as a multi-line string.       
def query_get_contacts(page=10, cursor=None):
  before = before_pattern(cursor)
  return """
    query getContacts {
      contacts(last: %d %s) { 
        total 
        edges { 
          cursor
          node { 
            id  
            firstName  
            lastName   
            email    
            mobile    
            gender  
            birthday 
            photo 
            twitter 
            instagram 
            linkedin 
            city 
            province 
            country 
            longitude 
            latitude 
            notes 
            totalSpent   
            tags(first: 10) {    
              nodes {   
                id   
                name    
              }  
            }  
          } 
        } 
      } 
    }
    """ % (page, before)

def query_get_conversations(page=10, cursor=None):
  before = before_pattern(cursor)
  return """
    query getConversations { 
      conversations(last: %d %s) { 
        total 
        edges { 
          cursor  
          node { 
            id 
            participant 
            contact { 
              id 
              firstName 
              lastName 
              mobile 
            } 
            platform 
            messages(last: 10) { 
              nodes { 
                id 
                body  
                direction 
                createdAt
              } 
            } 
          } 
        } 
      } 
    }
    """ % (page, before)


def update_pointers(lat, lng, cx, cy):
  global df_pointers
  if ((df_pointers['Latitude'] == lat) & (df_pointers['Longitude'] == lng)).any():
    df_pointers.loc[(df_pointers['Latitude'] == lat) & (df_pointers['Longitude'] == lng),'Numbers'] = df_pointers.loc[(df_pointers['Latitude'] == lat) & (df_pointers['Longitude'] == lng),'Numbers'] + 1
  else:
    df_pointers = df_pointers.append({'Latitude':lat,'Longitude':lng,'Numbers':1,'CX':cx,'CY':cy}, ignore_index=True)


join_with_none = lambda list_with_none, seperator=', ': seperator.join(str(v) for v in list_with_none)

def convert_contact(node):
  contact = {}
  # contact['id'] = node['id']
  contact['Name'] = node['firstName'] + ' ' + node['lastName']
  contact['Gender'] = node['gender']
  contact['photo link'] = node['photo']
  contact['City'] = node['city']
  contact['State'] = node['province']
  contact['Tags'] = join_with_none(node['tags']['nodes'])
  # contact['LAST CONTACTED'] = None
  contact['$ SPENT'] = node['totalSpent']
  contact['Email'] = node['email']
  # contact['Messaging'] = None
  contact['Mobile'] = node['mobile']
  # contact['Assigned'] = None
  contact['Address'] = join_with_none([node['city'], node['province'], node['country']])
  contact['Instagram'] = node['instagram']
  contact['Twitter'] = node['twitter']
  contact['Birthday'] = node['birthday']
  # contact['Industry'] = None
  contact['Notes'] = node['notes']
  # contact['Number of Messages Incoming'] = None
  # contact['Number of Messages Outgoing'] = None
  contact['Etc'] = None

  try:
    lat = int(node['latitude'] / 10) * 10
    lng = int(node['longitude'] / 10) * 10
    update_pointers(lat, lng, node['latitude'], node['longitude'])
  except:
    pass

  return contact

def convert_conversation(node):
  conversation = {}
  # conversation['id'] = node['id']
  try:
    conversation['Contact Name'] = node['contact']['firstName'] + " " + node['contact']['lastName']
  except:
    conversation['Contact Name'] = ""

  conversation['Messages Incoming'] = 0
  conversation['Messages outgoing'] = 0
  conversation['Message'] = ""
  conversation['Date'] = ""
      
  # conversation['Link to conversation in Database'] = None
  conversation['Contact Phone Number'] = node['participant']
  # conversation['Phone Number Assigned to User'] = None

  if len(node['messages']['nodes']) > 0:
    conversations = []

    for msg in node['messages']['nodes']:
      try:
        conversation['Message'] = msg['body']
        conversation['Date'] = msg['createdAt']
      except Exception as e:
        print(e)

      if msg['direction'] == "OUTGOING_TEXT":
        conversation['Messages outgoing'] = conversation['Messages outgoing'] + 1
      else:
        conversation['Messages Incoming'] = conversation['Messages Incoming'] + 1
      
      conversations.append(conversation.copy())
      # print(conversations, msg)

    return conversations

  return conversation


def get_all_contents(key, page=100, cursor=None, next_flag=True):
  global df_contact, df_conversation, cursors

  # switch contact | conversation
  if key == "contacts":
    query = query_get_contacts
    convert = convert_contact
    df = df_contact
  else:
    query = query_get_conversations
    convert = convert_conversation
    df = df_conversation

  # Execute the query contacts
  try:
    result = run_query(query(page=page, cursor=cursor))
    total = result["data"][key]["total"]
    datas = result["data"][key]["edges"]

    # convert dict list
    for data in datas:
      node = data['node']

      dict_node = convert(node)
      # print(dict_node)

      if type(dict_node) == type([]):
        for dict_node_one in dict_node:
          df = df.append(dict_node_one, ignore_index=True)
      else:
        df = df.append(dict_node, ignore_index=True)
      # print(df)

    if key == "contacts": df_contact = df 
    else: df_conversation = df
    print(df.size)

    # next cursor
    if next_flag and total > 0 and len(datas) > 0:
      last = datas[len(datas)-1]["cursor"]
      print(last)
      if last not in cursors:
        cursors.append(last)
        get_all_contents(key=key, page=page, cursor=last, next_flag=next_flag)
  except Exception as e:
    print(e)


def  get_upload_contacts():
  global df_contact, cursors
  cursors = []
  print("start to get contacts...")
  get_all_contents(key="contacts")
  # print(df_contact)
  # print(df_pointers)
  # upload_with_pd(df_contact, "Contacts")
  upload_with_pd(df_pointers, "Pointers")
  print("end to upload contacts...")

def  get_upload_conversations():
  global df_conversation, cursors
  cursors = []
  print("start to get conversations...")
  get_all_contents(key="conversations")
  # print(df_conversation)
  upload_with_pd(df_conversation, "Messages")
  print("end to upload conversations.")


def main():
  get_upload_contacts()
  # get_upload_conversations()
  

if __name__ == "__main__":
  main()