import requests
import pandas as pd
from gspread_pandas import upload_with_pd
from dotenv import dotenv_values


config = dotenv_values(".env")

PUBLIC_KEY = config['PUBLIC_KEY']
headers = {"Authorization": "Bearer {}".format(PUBLIC_KEY)}
URL = "https://api.superphone.io/graphql"
PAGE_STEP = 25
DOWNLOAD_STEP = 100
REQUEST_TIME_OUT = 300


# pandas data
# df = pd.DataFrame()

df_contact = pd.DataFrame(columns=[
  'id', 
  'Name', 
  'Gender', 
  'Photo', 
  'City', 
  'State', 
  'Tags', 
  # 'LAST CONTACTED', 
  'SPENT', 
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
  'cursor',
])

df_conversation = pd.DataFrame(columns=[
  'id',
  'Name',
  'Incoming',
  'outgoing',
  'Message',
  'Date',
  # 'Link to conversation in Database',
  'Phone',
  # 'Phone Number Assigned to User',
  'cursor',
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
# total count
total = 0
# first cursor
first = ""
# last cursor
last = ""

# callback function to save pointers
save_pointers = None

# callback function to save conversations
save_conversations = None

# callback function to save contacts
save_contacts = None


# lambdas
before_pattern = lambda cursor: "" if cursor is None else ", before: \"{}\"".format(cursor)
after_pattern = lambda cursor: "" if cursor is None else ", after: \"{}\"".format(cursor)
first_pattern = lambda page: "first: {}".format(page)
last_pattern = lambda page: "last: {}".format(page)

join_with_none = lambda list_with_none, seperator=', ': seperator.join(str(v) for v in list_with_none)


# run query
# A simple function to use requests.post to make the API call. Note the json= section.
def run_query(query):
  request = requests.post(URL, json={'query': query}, headers=headers, timeout=REQUEST_TIME_OUT)
  if request.status_code == 200:
    return request.json()
  else:
    raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))

        
# The GraphQL query (with a few aditional bits included) itself defined as a multi-line string.       
def query_get_contacts(page=10, cursor=None, isFirst=False, isBefore=True):
  page_pattern = first_pattern(page) if isFirst else last_pattern(page)
  cursor_pattern = before_pattern(cursor) if isBefore else after_pattern(cursor)
  return """
    query getContacts {
      contacts(%s %s) { 
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
    """ % (page_pattern, cursor_pattern)

def query_get_conversations(page=10, cursor=None, isFirst=False, isBefore=True):
  page_pattern = first_pattern(page) if isFirst else last_pattern(page)
  cursor_pattern = before_pattern(cursor) if isBefore else after_pattern(cursor)
  return """
    query getConversations { 
      conversations(%s %s) { 
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
              photo
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
    """ % (page_pattern, cursor_pattern)

# GraphQL mutation
def mutation_remove_conversation(id):
  return """
    mutation removeConversation {
      removeConversation(input: {
        conversationId: "%s"
      }) {
        removedConversationId
        conversationUserErrors {
          field
          message
        }
      }
    }
    """ % (id)

def mutation_remove_contact(id):
  return """
    mutation removeContact {
      removeContact(input: {
        contactId: "%s"
      }) {
        removedContactId
        contactUserErrors {
          field
          message
        }
      }
    }
    """ % (id)

def mutation_send_message(phone, message):
  return """
    mutation sendMessage {
      sendMessage(input: {
        mobile: "%s",
        platform: TWILIO,
        body: "%s"
      }) {
        message {
          id
        }
        sendMessageUserErrors {
          field
          message
        }
      }
    }
    """ % (phone, message)


# update df_pointers according to latitude & longitude
def update_pointers(lat, lng, cx, cy):
  global df_pointers
  if ((df_pointers['Latitude'] == lat) & (df_pointers['Longitude'] == lng)).any():
    df_pointers.loc[(df_pointers['Latitude'] == lat) & (df_pointers['Longitude'] == lng),'Numbers'] = df_pointers.loc[(df_pointers['Latitude'] == lat) & (df_pointers['Longitude'] == lng),'Numbers'] + 1
  else:
    df_pointers = df_pointers.append({'Latitude':lat,'Longitude':lng,'Numbers':1,'CX':cx,'CY':cy}, ignore_index=True)


# convert contact from node
def convert_contact(node):
  contact = {}
  contact['id'] = node['id']
  contact['Name'] = node['firstName'] + ' ' + node['lastName']
  contact['Gender'] = node['gender']
  contact['Photo'] = node['photo']
  contact['City'] = node['city']
  contact['State'] = node['province']
  contact['Tags'] = join_with_none(node['tags']['nodes'])
  # contact['LAST CONTACTED'] = None
  contact['SPENT'] = node['totalSpent']
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

# convert conversation from node
def convert_conversation(node, flag_multiple=True):
  conversation = {}
  conversation['id'] = node['id']
  try:
    conversation['Name'] = node['contact']['firstName'] + " " + node['contact']['lastName']
    conversation['Photo'] = node['contact']['photo']
  except:
    conversation['Name'] = ""
    conversation['Photo'] = ""
  

  conversation['Incoming'] = 0
  conversation['outgoing'] = 0
  conversation['Message'] = ""
  conversation['Date'] = ""
      
  # conversation['Link to conversation in Database'] = None
  conversation['Phone'] = node['participant']
  # conversation['Phone Number Assigned to User'] = None

  if len(node['messages']['nodes']) > 0:
    if flag_multiple:
      conversations = []

      for msg in node['messages']['nodes']:
        try:
          conversation['Message'] = msg['body']
          conversation['Date'] = msg['createdAt']
        except Exception as e:
          print(e)

        if "OUTGOING_" in msg['direction']:
          conversation['outgoing'] = conversation['outgoing'] + 1
        else:
          conversation['Incoming'] = conversation['Incoming'] + 1
        
        conversations.append(conversation.copy())
        # print(conversations, msg)

      return conversations

    else:
      msg = node['messages']['nodes'][0]
      try:
        conversation['Message'] = msg['body']
        conversation['Date'] = msg['createdAt']
        conversation['messages'] = []

        for msg in node['messages']['nodes']:
          conversation['messages'].append({
            'Message' : msg['body'],
            'Date' : msg['createdAt'],
            'direction' : msg['direction'],
          })
      except Exception as e:
        print(e)

  return conversation


# get all conversations & contacts
def get_all_contents(key, 
  page=DOWNLOAD_STEP, 
  cursor=None, 
  next_flag=True, 
  flag_conversation_multiple=True, 
  flag_clear_df=False,
  flag_last_order=True):

  global df_contact, df_conversation, cursors, total, first, last

  total = 0
  first = ""
  last = ""

  # switch contact | conversation
  if key == "contacts":
    query = query_get_contacts
    convert = convert_contact
    df = df_contact
  else:
    query = query_get_conversations
    convert = convert_conversation
    df = df_conversation
  
  # check flag to clear df
  if flag_clear_df : df = df.iloc[0:0]
  print("before df size", df.size)

  # Execute the query contacts
  try:
    if flag_last_order:
      query_result = query(page=page, cursor=cursor)
    else:
      query_result = query(page=page, cursor=cursor, isFirst=True, isBefore=False)
    # print("query is ", query_result)

    result = run_query(query_result)

    total = result["data"][key]["total"]
    datas = result["data"][key]["edges"]

    # convert dict list
    for data in datas:
      node = data['node']

      # check multiple conversations
      if not flag_conversation_multiple and key == "conversations":
        dict_node = convert(node, flag_multiple=False)
      else:
        dict_node = convert(node)
      # print(dict_node)

      if type(dict_node) == type([]):
        for dict_node_one in dict_node:
          # add cursor to dict_node
          dict_node_one['cursor'] = data['cursor']
          df = df.append(dict_node_one, ignore_index=True)
      else:
        # add cursor to dict_node
        dict_node['cursor'] = data['cursor']
        df = df.append(dict_node, ignore_index=True)
      # print(df)

    if key == "contacts": df_contact = df 
    else: df_conversation = df
    print("after df size", df.size)

    # first & last cursor
    if len(datas) > 0:
      first = datas[0]["cursor"]
      last = datas[len(datas)-1]["cursor"]
      print("first cursor", first)
      print("last cursor", last)
    
    # next cursor
    if next_flag and total > 0 and len(datas) > 0:
      if last not in cursors:
        cursors.append(last)
        get_all_contents(
          key=key, 
          page=page, 
          cursor=last, 
          next_flag=next_flag, 
          flag_conversation_multiple=flag_conversation_multiple,
          flag_last_order=flag_last_order)
  except Exception as e:
    print(e)

  return df

# remove conversation & contact
def remove_content(key, id):
  # switch contact | conversation
  if key == "contacts":
    mutation = mutation_remove_contact
  else:
    mutation = mutation_remove_conversation
  
  try:
    result = run_query(mutation(id=id))
  except Exception as e:
    print(e)
    result = None
  
  return result

# send message
def send_message(phone, message):
  try:
    result = run_query(mutation_send_message(phone=phone, message=message))
  except Exception as e:
    print(e)
    result = None
  
  return result


# get & upload & save contacts & pointers
def get_upload_contacts(isClear=False, isSave=False, isUpload=True, cursor=None, page=PAGE_STEP):
  global df_contact, df_pointers, cursors, total, last
  cursors = []

  print("start to get contacts...")
  if isClear :
    get_all_contents(
      key="contacts",
      page=page, 
      cursor=cursor, 
      next_flag=False, 
      flag_conversation_multiple=False,
      flag_clear_df=True)
  else:
    get_all_contents(key="contacts")
  # print(df_contact)
  # print(df_pointers)
  print("end to get contacts and pointers.")

  if isSave and callable(save_contacts) :
    save_contacts(df_contact)
    print("end to upload contacts.")

  if isSave and callable(save_pointers) :
    save_pointers(df_pointers)
    print("end to upload pointers.")

  if isUpload :
    upload_with_pd(df_contact.loc[:,'Name':'Etc'], "Contacts", isClear)
    upload_with_pd(df_pointers, "Pointers", True)
    print("end to upload contacts and pointers.")

  return df_contact, total, last

# get & upload & save conversations
def get_upload_conversations(isClear=False, isSave=False, isUpload=True, cursor=None, page=PAGE_STEP):
  global df_conversation, cursors, total, last
  cursors = []

  print("start to get conversations...")
  if isClear :
    get_all_contents(
      key="conversations",
      page=page, 
      cursor=cursor, 
      next_flag=False, 
      flag_conversation_multiple=False,
      flag_clear_df=True)
  else :
    get_all_contents(key="conversations")
  # print(df_conversation)
  print("end to get conversations.")

  if isSave and callable(save_conversations) :
    save_conversations(df_conversation)
    print("end to save conversations.")
  
  if isUpload :
    upload_with_pd(df_conversation.loc[:,'Name':'Phone'], "Messages", isClear)
    print("end to upload conversations.")

  return df_conversation, total, last


# set save pointers callback
def set_save_pointers(callback_func):
  global save_pointers
  save_pointers = callback_func

# set save contacts callback
def set_save_contacts(callback_func):
  global save_contacts
  save_contacts = callback_func

# set save conversations callback
def set_save_conversations(callback_func):
  global save_conversations
  save_conversations = callback_func


# main
def main():
  get_upload_contacts()
  # get_upload_conversations()


if __name__ == "__main__":
  main()